use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, Decimal128Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    cast::AsArray,
};
use flow_iceberg_ext::{ArtifactSet, ArtifactTracker, TargetPartitions};
use flow_model::{
    ColumnType, FileId, OperationId, PgLsn, Row, RowLocation, TableSchema, Value, fingerprint,
};
use futures::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use iceberg::{
    arrow::{PartitionValueCalculator, arrow_struct_to_literal, schema_to_arrow_schema},
    io::FileIO,
    metadata_columns::{RESERVED_FIELD_ID_DELETE_FILE_PATH, RESERVED_FIELD_ID_DELETE_FILE_POS},
    spec::{
        DataContentType, DataFile, Datum, Literal, NestedField, PartitionSpec, PrimitiveType,
        Schema, Struct, Type,
    },
    writer::{
        CurrentFileStatus,
        file_writer::{FileWriter, FileWriterBuilder, ParquetWriter, ParquetWriterBuilder},
    },
};
use parquet::{
    basic::{Compression, ZstdLevel},
    file::properties::{EnabledStatistics, WriterProperties},
};
use std::{
    borrow::Borrow,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering as AtomicOrdering},
    },
};

use crate::{
    arrow::{validated_columns, writer_arrow_schema},
    iceberg_schema,
    sort::{SortSpec, SortVerifier},
};

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub target_file_bytes: usize,
    pub row_group_rows: usize,
    /// Rows per batch when compaction reads and rewrites files, independent of
    /// how many rows each written row group holds.
    pub batch_rows: usize,
    /// Logical input bytes per Arrow batch, including offsets and validity allowance.
    /// This is not a bound on codec buffers or process memory.
    pub batch_bytes: usize,
    /// Logical input bytes retained in a row group before it is flushed.
    pub row_group_bytes: usize,
    pub compression: Compression,
    pub artifact_tracker: Option<Arc<dyn ArtifactTracker>>,
}
impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            target_file_bytes: 16 * 1024 * 1024,
            row_group_rows: 8192,
            batch_rows: 8192,
            batch_bytes: 8 * 1024 * 1024,
            row_group_bytes: 32 * 1024 * 1024,
            compression: Compression::ZSTD(ZstdLevel::default()),
            artifact_tracker: None,
        }
    }
}
impl WriterConfig {
    fn properties(&self) -> Result<WriterProperties> {
        self.properties_sorted(None)
    }

    fn properties_sorted(
        &self,
        sorting: Option<Vec<parquet::file::metadata::SortingColumn>>,
    ) -> Result<WriterProperties> {
        ensure!(
            self.target_file_bytes > 0
                && self.row_group_rows > 0
                && self.batch_rows > 0
                && self.batch_bytes > 0
                && self.row_group_bytes > 0,
            "writer limits must be positive"
        );
        ensure!(
            self.batch_limit() <= i32::MAX as usize,
            "writer batch limit exceeds Arrow's variable-width offset range"
        );
        // Page statistics add the column index: per-page bounds that let
        // readers skip pages inside a row group, not just whole row groups.
        // Every file keeps the offset index, which position reads also use.
        Ok(WriterProperties::builder()
            .set_compression(self.compression)
            .set_max_row_group_row_count(Some(self.row_group_rows))
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_sorting_columns(sorting)
            .build())
    }

    fn batch_limit(&self) -> usize {
        self.batch_bytes.min(self.row_group_bytes)
    }

    fn admit_row(&self, bytes: usize) -> Result<()> {
        ensure!(
            bytes <= self.batch_limit(),
            "row logical payload {bytes} bytes exceeds writer admission limit {} bytes",
            self.batch_limit()
        );
        Ok(())
    }
}

/// Charge fixed-width slots even for nulls, one validity byte per field, and
/// two i32 offsets per variable-width value. The offset allowance also covers
/// the final offset of a one-row Arrow array. Allocator capacity is excluded.
fn fixed_bytes(data_type: &ColumnType) -> usize {
    1 + match data_type {
        ColumnType::Bool => 1,
        ColumnType::Int32 | ColumnType::Date => 4,
        ColumnType::Int64
        | ColumnType::Float64
        | ColumnType::TimestampMicros
        | ColumnType::TimestampTzMicros
        | ColumnType::String
        | ColumnType::Binary => 8,
        ColumnType::Uuid | ColumnType::Decimal { .. } => 16,
    }
}

fn row_bytes(schema: &TableSchema, row: &Row) -> Result<usize> {
    schema
        .columns
        .iter()
        .zip(row)
        .try_fold(0usize, |bytes, (column, value)| {
            let variable = match value {
                Value::String(value) => value.len(),
                Value::Binary(value) => value.len(),
                _ => 0,
            };
            bytes
                .checked_add(fixed_bytes(&column.data_type))
                .and_then(|bytes| bytes.checked_add(variable))
                .context("row logical payload size overflow")
        })
}

/// [`row_bytes`] for each row of a batch in the writer's Arrow schema.
fn batch_row_bytes(schema: &TableSchema, batch: &RecordBatch) -> Result<Vec<usize>> {
    let fixed: usize = schema
        .columns
        .iter()
        .map(|column| fixed_bytes(&column.data_type))
        .sum();
    let mut bytes = vec![fixed; batch.num_rows()];
    for (column, array) in schema.columns.iter().zip(batch.columns()) {
        // Offsets give each value's length without touching its payload.
        let offsets: Vec<usize> = match column.data_type {
            ColumnType::String => array
                .as_string_opt::<i32>()
                .map(|array| array.offsets().windows(2).map(|r| (r[1] - r[0]) as usize))
                .context("string type mismatch")?
                .collect(),
            ColumnType::Binary => array
                .as_binary_opt::<i64>()
                .map(|array| array.offsets().windows(2).map(|r| (r[1] - r[0]) as usize))
                .context("binary type mismatch")?
                .collect(),
            _ => continue,
        };
        for (index, (bytes, length)) in bytes.iter_mut().zip(offsets).enumerate() {
            if array.is_valid(index) {
                *bytes = bytes
                    .checked_add(length)
                    .context("row logical payload size overflow")?;
            }
        }
    }
    Ok(bytes)
}

/// One position for a delete writer: an owned [`RowLocation`], or a borrowed
/// `(data file path, row position)` pair, which avoids an owned path per
/// position. The writer finds the target's spec and partition by its path.
pub trait DeletePosition {
    fn data_file(&self) -> &str;
    fn row_position(&self) -> u64;
}

impl DeletePosition for RowLocation {
    fn data_file(&self) -> &str {
        &self.data_file_id.0
    }
    fn row_position(&self) -> u64 {
        self.row_position
    }
}

impl DeletePosition for (&str, u64) {
    fn data_file(&self) -> &str {
        self.0
    }
    fn row_position(&self) -> u64 {
        self.1
    }
}

fn delete_bytes(position: &impl DeletePosition) -> Result<usize> {
    // String offsets, Int64 position, and a validity byte for each column.
    position
        .data_file()
        .len()
        .checked_add(18)
        .context("delete row logical payload size overflow")
}

fn bounded_batch<T>(
    items: &[T],
    config: &WriterConfig,
    size: impl Fn(&T) -> Result<usize>,
) -> Result<(usize, usize)> {
    let mut count = 0;
    let mut bytes = 0;
    for item in items.iter().take(config.row_group_rows) {
        let next = size(item)?;
        config.admit_row(next)?;
        if next > config.batch_limit() - bytes {
            break;
        }
        count += 1;
        bytes += next;
    }
    Ok((count, bytes))
}

#[derive(Default)]
struct RowGroupBudget {
    rows: usize,
    bytes: usize,
}

impl RowGroupBudget {
    async fn prepare(
        &mut self,
        writer: &mut ParquetWriter,
        rows: usize,
        bytes: usize,
        config: &WriterConfig,
    ) -> Result<()> {
        if rows > config.row_group_rows - self.rows || bytes > config.row_group_bytes - self.bytes {
            writer.flush_row_group().await?;
            *self = Self::default();
        }
        Ok(())
    }

    fn written(&mut self, rows: usize, bytes: usize, config: &WriterConfig) {
        self.rows += rows;
        self.bytes += bytes;
        // Parquet's configured row limit flushes exactly at this boundary.
        if self.rows == config.row_group_rows {
            *self = Self::default();
        }
    }
}

/// How a [`DataWriter`] treats the table's sort order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortMode {
    /// Check every row and stamp only the files that turned out sorted.
    Verify,
    /// The caller sorted the rows: also declare Parquet sorting columns, and
    /// fail rather than write a row out of order.
    Sorted,
}

struct WriterSort {
    spec: SortSpec,
    mode: SortMode,
}

/// Open files a partitioned writer keeps at once. Opening another closes the
/// least recently written one, so a write spanning more partitions writes
/// more, smaller files instead of holding a file open for each.
const MAX_OPEN_PARTITION_FILES: usize = 64;

/// Row groups a partitioned writer buffers across its open files, in units
/// of `row_group_bytes`. Past it the largest buffered row group is flushed.
const PARTITION_BUFFER_ROW_GROUPS: usize = 4;

/// Computes each row's partition under a partitioned spec.
struct Partitioner {
    calculator: PartitionValueCalculator,
}

/// One open output file and the partition all its rows belong to.
struct OpenFile {
    key: Vec<u8>,
    partition: Struct,
    writer: ParquetWriter,
    row_group: RowGroupBudget,
    verifier: Option<SortVerifier>,
    /// Whether every row of this file so far was in order.
    sorted: bool,
    last_write: u64,
}

/// One file is rotated only between batches, so positions are known as rows are written.
/// An operation ID is immutable: retries must reuse prepared artifacts, never overwrite them.
///
/// Under a partitioned spec (see [`Self::with_partition_spec`]) each output
/// file holds one partition, and a batch's rows go to their partitions' files.
pub struct DataWriter {
    file_io: FileIO,
    schema: TableSchema,
    /// The Iceberg schema files are written in, including lineage columns.
    file_schema: Arc<Schema>,
    builder: ParquetWriterBuilder,
    sort: Option<WriterSort>,
    config: WriterConfig,
    prefix: String,
    next_file: usize,
    open: Vec<OpenFile>,
    files: Vec<DataFile>,
    spec_id: i32,
    partitioner: Option<Partitioner>,
    writes: u64,
    arrow_schema: arrow_schema::SchemaRef,
    lineage_schema: Option<arrow_schema::SchemaRef>,
}
#[derive(Debug)]
pub struct WrittenBatch {
    pub locations: Vec<RowLocation>,
}
impl DataWriter {
    pub fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        schema: TableSchema,
        spec_id: i32,
        config: WriterConfig,
    ) -> Result<Self> {
        ensure!(spec_id >= 0, "invalid spec ID");
        let file_schema = Arc::new(iceberg_schema(&schema)?);
        let builder = ParquetWriterBuilder::new(config.properties()?, file_schema.clone());
        ensure!(
            !operation.0.is_empty()
                && operation
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid operation ID for artifact path"
        );
        Ok(Self {
            file_io,
            arrow_schema: writer_arrow_schema(&schema),
            schema,
            file_schema,
            builder,
            sort: None,
            config,
            prefix: format!("{}/data/{}", location.trim_end_matches('/'), operation.0),
            next_file: 0,
            open: Vec::new(),
            files: Vec::new(),
            spec_id,
            partitioner: None,
            writes: 0,
            lineage_schema: None,
        })
    }

    /// Write files in `spec`, whose ID replaces the one given to [`Self::new`].
    /// A partitioned spec splits rows by their transformed values; its source
    /// fields must be columns of the writer's schema.
    pub fn with_partition_spec(mut self, spec: &PartitionSpec) -> Result<Self> {
        ensure!(
            self.open.is_empty() && self.files.is_empty(),
            "partition spec must be configured before writing"
        );
        ensure!(spec.spec_id() >= 0, "invalid spec ID");
        self.spec_id = spec.spec_id();
        self.partitioner = if spec.is_unpartitioned() {
            None
        } else {
            let schema = iceberg_schema(&self.schema)?;
            Some(Partitioner {
                calculator: PartitionValueCalculator::try_new(spec, &schema)
                    .context("partition spec does not apply to the table schema")?,
            })
        };
        Ok(self)
    }

    /// Preserve reserved Iceberg row lineage while rewriting existing rows.
    pub fn with_row_lineage(mut self) -> Result<Self> {
        ensure!(
            self.open.is_empty() && self.files.is_empty(),
            "lineage must be configured before writing"
        );
        let schema = crate::lineage::writer_schema(&self.schema)?;
        self.lineage_schema = Some(Arc::new(schema_to_arrow_schema(&schema)?));
        self.file_schema = Arc::new(schema);
        self.rebuild()?;
        Ok(self)
    }

    /// Write files in `spec`'s order. With [`SortMode::Verify`], a file is
    /// stamped with the order only if all its rows were in order; with
    /// [`SortMode::Sorted`], a row out of order is an error.
    pub fn with_sort(mut self, spec: &SortSpec, mode: SortMode) -> Result<Self> {
        ensure!(
            self.open.is_empty() && self.files.is_empty(),
            "sort order must be configured before writing"
        );
        ensure!(
            spec.keys
                .iter()
                .all(|key| key.column < self.schema.columns.len()),
            "sort column is outside the table schema"
        );
        spec.iceberg_order_id()?;
        self.sort = Some(WriterSort {
            spec: spec.clone(),
            mode,
        });
        self.rebuild()?;
        Ok(self)
    }

    fn rebuild(&mut self) -> Result<()> {
        let sorting = match &self.sort {
            Some(sort) if sort.mode == SortMode::Sorted => Some(sort.spec.sorting_columns()?),
            _ => None,
        };
        self.builder = ParquetWriterBuilder::new(
            self.config.properties_sorted(sorting)?,
            self.file_schema.clone(),
        );
        Ok(())
    }

    /// Accept owned or borrowed rows without copying their payloads.
    pub async fn write(
        &mut self,
        rows: &[impl Borrow<Row>],
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        self.write_with_lineage(rows, None, source_lsn).await
    }

    pub async fn write_with_lineage(
        &mut self,
        rows: &[impl Borrow<Row>],
        lineage: Option<&[crate::RowLineage]>,
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        ensure!(
            lineage.is_none_or(|values| values.len() == rows.len()),
            "row lineage length mismatch"
        );
        ensure!(
            lineage.is_none() || self.lineage_schema.is_some(),
            "writer is not configured for row lineage"
        );
        if rows.is_empty() {
            return Ok(WrittenBatch { locations: vec![] });
        }
        // Validate the entire call before opening any output. A late oversized
        // row must not leave a partially written artifact behind.
        for row in rows {
            let row = row.borrow();
            self.schema.validate_row(row)?;
            self.config.admit_row(
                row_bytes(&self.schema, row)? + usize::from(self.lineage_schema.is_some()) * 18,
            )?;
        }
        let mut locations = Vec::with_capacity(rows.len());
        let mut remaining = rows;
        while !remaining.is_empty() {
            let (count, bytes) = bounded_batch(remaining, &self.config, |row| {
                Ok(row_bytes(&self.schema, row.borrow())?
                    + usize::from(self.lineage_schema.is_some()) * 18)
            })?;
            let rows = &remaining[..count];
            let offset = locations.len();
            // Rows were validated when the write call was admitted.
            let fingerprints: Vec<_> = rows.iter().map(|row| fingerprint(row.borrow())).collect();
            self.write_partitioned(
                validated_columns(&self.schema, rows, true)?,
                count,
                lineage.map(|values| &values[offset..offset + count]),
                &fingerprints,
                bytes,
                source_lsn,
                &mut locations,
            )
            .await?;
            remaining = &remaining[count..];
        }
        Ok(WrittenBatch { locations })
    }

    /// Write rows already in [`crate::writer_arrow_schema`], such as rewritten scan
    /// output, without converting them through [`Row`]. Applies the same
    /// validation and admission limits as [`Self::write_with_lineage`].
    /// `fingerprints` are the caller's [`flow_model::fingerprint`] per row.
    pub async fn write_arrow(
        &mut self,
        batch: &RecordBatch,
        lineage: Option<&[crate::RowLineage]>,
        fingerprints: &[[u8; 16]],
        source_lsn: PgLsn,
    ) -> Result<WrittenBatch> {
        let rows = batch.num_rows();
        ensure!(
            lineage.is_none_or(|values| values.len() == rows) && fingerprints.len() == rows,
            "row lineage or fingerprint length mismatch"
        );
        ensure!(
            lineage.is_none() || self.lineage_schema.is_some(),
            "writer is not configured for row lineage"
        );
        ensure!(
            batch.schema().fields() == self.arrow_schema.fields(),
            "batch is not in the writer's Arrow schema"
        );
        if rows == 0 {
            return Ok(WrittenBatch { locations: vec![] });
        }
        // The Arrow schema already fixes types and required fields. Decimal
        // precision is the remaining check `validate_row` would apply.
        for (column, array) in self.schema.columns.iter().zip(batch.columns()) {
            if let ColumnType::Decimal { precision, .. } = column.data_type {
                array
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .context("decimal type mismatch")?
                    .validate_decimal_precision(precision)
                    .with_context(|| format!("type or nullability mismatch for {}", column.name))?;
            }
        }
        let lineage_bytes = usize::from(self.lineage_schema.is_some()) * 18;
        let sizes: Vec<_> = batch_row_bytes(&self.schema, batch)?
            .into_iter()
            .map(|bytes| bytes + lineage_bytes)
            .collect();
        // Admit the whole call before opening any output, as the row path does.
        for bytes in &sizes {
            self.config.admit_row(*bytes)?;
        }
        let mut locations = Vec::with_capacity(rows);
        let mut offset = 0;
        while offset < rows {
            let (count, bytes) = bounded_batch(&sizes[offset..], &self.config, |bytes| Ok(*bytes))?;
            self.write_partitioned(
                batch.slice(offset, count).columns().to_vec(),
                count,
                lineage.map(|values| &values[offset..offset + count]),
                &fingerprints[offset..offset + count],
                bytes,
                source_lsn,
                &mut locations,
            )
            .await?;
            offset += count;
        }
        Ok(WrittenBatch { locations })
    }

    /// Route a bounded batch to its partitions' files. Locations are appended
    /// in the batch's row order.
    #[allow(clippy::too_many_arguments)]
    async fn write_partitioned(
        &mut self,
        columns: Vec<ArrayRef>,
        rows: usize,
        lineage: Option<&[crate::RowLineage]>,
        fingerprints: &[[u8; 16]],
        bytes: usize,
        source_lsn: PgLsn,
        locations: &mut Vec<RowLocation>,
    ) -> Result<()> {
        let Some(partitioner) = &self.partitioner else {
            return self
                .write_batch(
                    Vec::new(),
                    Struct::empty(),
                    columns,
                    rows,
                    lineage,
                    fingerprints,
                    bytes,
                    source_lsn,
                    locations,
                )
                .await;
        };
        let values = partitioner
            .calculator
            .calculate(&RecordBatch::try_new(
                self.arrow_schema.clone(),
                columns.clone(),
            )?)
            .context("compute partition values")?;
        let tuples = arrow_struct_to_literal(&values, partitioner.calculator.partition_type())
            .context("read partition values")?;
        // Group rows by partition, keeping each group's rows in batch order.
        // Neighbouring rows usually share a partition; check the last first.
        let mut groups: Vec<(Vec<u8>, Struct, Vec<u32>)> = Vec::new();
        let mut index = std::collections::HashMap::<Vec<u8>, usize>::new();
        let mut last: Option<usize> = None;
        for (row, tuple) in tuples.into_iter().enumerate() {
            let partition = match tuple {
                Some(Literal::Struct(partition)) => partition,
                _ => anyhow::bail!("partition values are not a struct"),
            };
            let key = flow_iceberg_ext::partition_key(&partition);
            let group = match last.filter(|&group| groups[group].0 == key) {
                Some(group) => group,
                None => *index.entry(key.clone()).or_insert_with(|| {
                    groups.push((key, partition, Vec::new()));
                    groups.len() - 1
                }),
            };
            last = Some(group);
            groups[group].2.push(row as u32);
        }
        if let [(key, partition, _)] = groups.as_mut_slice() {
            let (key, partition) = (
                std::mem::take(key),
                std::mem::replace(partition, Struct::empty()),
            );
            return self
                .write_batch(
                    key,
                    partition,
                    columns,
                    rows,
                    lineage,
                    fingerprints,
                    bytes,
                    source_lsn,
                    locations,
                )
                .await;
        }
        let mut placed: Vec<Option<RowLocation>> = vec![None; rows];
        for (key, partition, indices) in groups {
            let take = UInt32Array::from(indices);
            let columns = columns
                .iter()
                .map(|column| arrow_select::take::take(column.as_ref(), &take, None))
                .collect::<Result<Vec<_>, _>>()?;
            let group_lineage: Option<Vec<_>> = lineage.map(|values| {
                take.values()
                    .iter()
                    .map(|&row| values[row as usize])
                    .collect()
            });
            let group_fingerprints: Vec<_> = take
                .values()
                .iter()
                .map(|&row| fingerprints[row as usize])
                .collect();
            let group_bytes = bytes.saturating_mul(take.len()).div_ceil(rows.max(1));
            let mut written = Vec::with_capacity(take.len());
            self.write_batch(
                key,
                partition,
                columns,
                take.len(),
                group_lineage.as_deref(),
                &group_fingerprints,
                group_bytes,
                source_lsn,
                &mut written,
            )
            .await?;
            for (&row, location) in take.values().iter().zip(written) {
                placed[row as usize] = Some(location);
            }
        }
        for location in placed {
            locations.push(location.context("partitioned write lost a row")?);
        }
        Ok(())
    }

    /// The index of the open file for `key`, opening one if needed.
    async fn open_file(&mut self, key: Vec<u8>, partition: Struct) -> Result<usize> {
        let existing = self.open.iter().position(|file| file.key == key);
        if let Some(index) = existing {
            if self.open[index].writer.current_written_size() < self.config.target_file_bytes {
                return Ok(index);
            }
            self.finish_file(index).await?;
        }
        if self.open.len() >= MAX_OPEN_PARTITION_FILES {
            let oldest = self
                .open
                .iter()
                .enumerate()
                .min_by_key(|(_, file)| file.last_write)
                .map(|(index, _)| index)
                .expect("open files at the limit");
            self.finish_file(oldest).await?;
        }
        let path = format!("{}-data-{:06}.parquet", self.prefix, self.next_file);
        self.next_file += 1;
        if let Some(tracker) = &self.config.artifact_tracker {
            tracker
                .register(ArtifactSet {
                    paths: vec![path.clone()],
                    ranges: Vec::new(),
                })
                .await?;
        }
        let writer = self
            .builder
            .build(self.file_io.new_output(path)?.if_absent())
            .await?;
        self.open.push(OpenFile {
            key,
            partition,
            writer,
            row_group: RowGroupBudget::default(),
            verifier: self.sort.as_ref().map(|sort| SortVerifier::new(&sort.spec)),
            sorted: true,
            last_write: 0,
        });
        Ok(self.open.len() - 1)
    }

    /// Flush buffered row groups, largest first, until the open files hold
    /// at most [`PARTITION_BUFFER_ROW_GROUPS`] row groups' bytes. A write to
    /// a few partitions keeps the usual row-group size.
    async fn bound_buffers(&mut self) -> Result<()> {
        if self.open.len() < 2 {
            return Ok(());
        }
        let budget = self
            .config
            .row_group_bytes
            .saturating_mul(PARTITION_BUFFER_ROW_GROUPS);
        let mut total: usize = self.open.iter().map(|file| file.row_group.bytes).sum();
        while total > budget {
            let (index, _) = self
                .open
                .iter()
                .enumerate()
                .max_by_key(|(_, file)| file.row_group.bytes)
                .expect("open files");
            let file = &mut self.open[index];
            total -= file.row_group.bytes;
            file.writer.flush_row_group().await?;
            file.row_group = RowGroupBudget::default();
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_batch(
        &mut self,
        key: Vec<u8>,
        partition: Struct,
        mut columns: Vec<ArrayRef>,
        rows: usize,
        lineage: Option<&[crate::RowLineage]>,
        fingerprints: &[[u8; 16]],
        bytes: usize,
        source_lsn: PgLsn,
        locations: &mut Vec<RowLocation>,
    ) -> Result<()> {
        let index = self.open_file(key, partition).await?;
        self.writes += 1;
        let writes = self.writes;
        let file = &mut self.open[index];
        file.last_write = writes;
        if let (Some(sort), Some(verifier)) = (&self.sort, &mut file.verifier)
            && file.sorted
            && !verifier.accept(&columns)?
        {
            ensure!(
                sort.mode == SortMode::Verify,
                "rows are out of the table's sort order {}",
                sort.spec.order_id
            );
            file.sorted = false;
        }
        file.row_group
            .prepare(&mut file.writer, rows, bytes, &self.config)
            .await?;
        let batch = if let Some(schema) = &self.lineage_schema {
            for row_id in [true, false] {
                let values = (0..rows).map(|index| {
                    lineage.and_then(|values| {
                        if row_id {
                            values[index].row_id
                        } else {
                            values[index].last_updated_sequence_number
                        }
                    })
                });
                columns.push(Arc::new(values.collect::<Int64Array>()));
            }
            RecordBatch::try_new(schema.clone(), columns)?
        } else {
            RecordBatch::try_new(self.arrow_schema.clone(), columns)?
        };
        let path = file.writer.current_file_path();
        let offset = file.writer.current_row_num() as u64;
        file.writer.write(&batch).await?;
        file.row_group.written(rows, bytes, &self.config);
        for (i, fingerprint) in fingerprints.iter().enumerate() {
            locations.push(RowLocation {
                data_file_id: FileId(path.clone()),
                row_position: offset + i as u64,
                data_sequence_number: -1,
                spec_id: self.spec_id,
                partition: vec![],
                source_commit_lsn: source_lsn,
                row_version: 0,
                row_fingerprint: *fingerprint,
            });
        }
        if self.partitioner.is_some() {
            self.bound_buffers().await?;
        }
        Ok(())
    }

    async fn finish_file(&mut self, index: usize) -> Result<()> {
        let file = self.open.remove(index);
        let order = match &self.sort {
            Some(sort) if file.sorted => Some(sort.spec.iceberg_order_id()?),
            _ => None,
        };
        for mut built in file.writer.close().await? {
            built
                .content(DataContentType::Data)
                .partition(file.partition.clone())
                .partition_spec_id(self.spec_id);
            if let Some(order) = order {
                built.sort_order_id(order);
            }
            self.files.push(built.build()?);
        }
        Ok(())
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        // Close in file order so the returned files follow their names.
        self.open
            .sort_by_key(|file| file.writer.current_file_path());
        while !self.open.is_empty() {
            self.finish_file(0).await?;
        }
        self.files
            .sort_by(|left, right| left.file_path().cmp(right.file_path()));
        Ok(self.files)
    }
}

/// Streaming position-delete writer. Input must be sorted globally by path and position.
/// Callers sort with the state store's disk-backed spool or, within a bounded
/// position budget, in memory.
pub(crate) struct ParquetDeleteWriter {
    file_io: FileIO,
    builder: ParquetWriterBuilder,
    arrow_schema: arrow_schema::SchemaRef,
    config: WriterConfig,
    prefix: String,
    /// Names the next file; shared by the writers of one operation.
    next_file: Arc<AtomicUsize>,
    current: Option<ParquetWriter>,
    row_group: RowGroupBudget,
    files: Vec<DataFile>,
    last: Option<(String, u64)>,
    file_paths: Option<(String, String)>,
    /// Spec and partition of every file; all its targets share them.
    partition: (i32, Struct),
    /// Files closing concurrently; see [`Self::finish_file_concurrently`].
    closing: FuturesUnordered<BoxFuture<'static, Result<Vec<DataFile>>>>,
    closed_concurrently: bool,
}

/// Small per-target files cost one object upload each. Overlap that many.
const MAX_CONCURRENT_CLOSES: usize = 8;

impl ParquetDeleteWriter {
    fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        partition: (i32, Struct),
        next_file: Arc<AtomicUsize>,
        config: WriterConfig,
    ) -> Result<Self> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_PATH,
                        "file_path",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::required(
                        RESERVED_FIELD_ID_DELETE_FILE_POS,
                        "pos",
                        Type::Primitive(PrimitiveType::Long),
                    )),
                ])
                .build()?,
        );
        let arrow_schema = Arc::new(schema_to_arrow_schema(&schema)?);
        let builder = ParquetWriterBuilder::new(config.properties()?, schema);
        ensure!(
            !operation.0.is_empty()
                && operation
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid operation ID"
        );
        Ok(Self {
            file_io,
            builder,
            arrow_schema,
            config,
            prefix: format!("{}/data/{}", location.trim_end_matches('/'), operation.0),
            next_file,
            current: None,
            row_group: RowGroupBudget::default(),
            files: vec![],
            last: None,
            file_paths: None,
            partition,
            closing: FuturesUnordered::new(),
            closed_concurrently: false,
        })
    }
    pub async fn write(&mut self, positions: &[impl DeletePosition]) -> Result<()> {
        if positions.is_empty() {
            return Ok(());
        }
        let mut last = self
            .last
            .as_ref()
            .map(|(path, position)| (path.as_str(), *position));
        for p in positions {
            ensure!(
                p.row_position() <= i64::MAX as u64,
                "position overflows Iceberg long"
            );
            self.config.admit_row(delete_bytes(p)?)?;
            if let Some((path, pos)) = last {
                ensure!(
                    (path, pos) < (p.data_file(), p.row_position()),
                    "delete positions must be sorted and unique"
                );
            }
            last = Some((p.data_file(), p.row_position()));
        }
        let mut remaining = positions;
        while !remaining.is_empty() {
            let (count, bytes) = bounded_batch(remaining, &self.config, delete_bytes)?;
            self.write_batch(&remaining[..count], bytes).await?;
            remaining = &remaining[count..];
        }
        let last = positions.last().expect("nonempty positions");
        self.last = Some((last.data_file().to_owned(), last.row_position()));
        Ok(())
    }

    async fn write_batch(&mut self, positions: &[impl DeletePosition], bytes: usize) -> Result<()> {
        if self
            .current
            .as_ref()
            .is_some_and(|w| w.current_written_size() >= self.config.target_file_bytes)
        {
            self.finish_file().await?;
        }
        if self.current.is_none() {
            let path = format!(
                "{}-delete-{:06}.parquet",
                self.prefix,
                self.next_file.fetch_add(1, AtomicOrdering::Relaxed)
            );
            if let Some(tracker) = &self.config.artifact_tracker {
                tracker
                    .register(ArtifactSet {
                        paths: vec![path.clone()],
                        ranges: Vec::new(),
                    })
                    .await?;
            }
            self.current = Some(
                self.builder
                    .build(self.file_io.new_output(path)?.if_absent())
                    .await?,
            );
        }
        let writer = self.current.as_mut().expect("writer opened above");
        self.row_group
            .prepare(writer, positions.len(), bytes, &self.config)
            .await?;
        let batch = RecordBatch::try_new(
            self.arrow_schema.clone(),
            vec![
                Arc::new(
                    positions
                        .iter()
                        .map(|p| Some(p.data_file()))
                        .collect::<StringArray>(),
                ),
                Arc::new(
                    positions
                        .iter()
                        .map(|p| Some(p.row_position() as i64))
                        .collect::<Int64Array>(),
                ),
            ],
        )?;
        writer.write(&batch).await?;
        self.row_group.written(positions.len(), bytes, &self.config);
        let first = positions.first().expect("nonempty batch").data_file();
        let last = positions.last().expect("nonempty batch").data_file();
        if let Some((_, upper)) = &mut self.file_paths {
            upper.clear();
            upper.push_str(last);
        } else {
            self.file_paths = Some((first.to_owned(), last.to_owned()));
        }
        Ok(())
    }

    /// Close the current file before admitting another sorted range of deletes.
    /// Calling this without an open file is a no-op. Global path/position ordering
    /// and uniqueness remain enforced across rotations; artifact names are never reused.
    pub async fn finish_file(&mut self) -> Result<()> {
        if let Some(close) = self.take_file()? {
            self.files.extend(close.await?);
        }
        Ok(())
    }

    /// [`Self::finish_file`], but the closed file's upload overlaps later
    /// writes, up to [`MAX_CONCURRENT_CLOSES`] at once. `close` waits for all
    /// of them; dropping the writer cancels unfinished uploads.
    pub async fn finish_file_concurrently(&mut self) -> Result<()> {
        if let Some(close) = self.take_file()? {
            self.closing.push(close);
            self.closed_concurrently = true;
            while self.closing.len() >= MAX_CONCURRENT_CLOSES {
                let files = self.closing.next().await.expect("pending close")?;
                self.files.extend(files);
            }
        }
        Ok(())
    }

    fn take_file(&mut self) -> Result<Option<BoxFuture<'static, Result<Vec<DataFile>>>>> {
        self.row_group = RowGroupBudget::default();
        let Some(writer) = self.current.take() else {
            return Ok(None);
        };
        let (first, last) = self.file_paths.take().context("delete file has no rows")?;
        let (spec_id, partition) = self.partition.clone();
        Ok(Some(Box::pin(async move {
            let mut files = Vec::new();
            for mut file in writer.close().await? {
                file.content(DataContentType::PositionDeletes)
                    .partition(partition.clone())
                    .partition_spec_id(spec_id);
                // Parquet's string statistics can be omitted or truncated. These
                // exact bounds let readers exclude unrelated delete-file ranges.
                let metadata = file.build()?;
                let mut lower = metadata.lower_bounds().clone();
                let mut upper = metadata.upper_bounds().clone();
                lower.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&first));
                upper.insert(RESERVED_FIELD_ID_DELETE_FILE_PATH, Datum::string(&last));
                file.lower_bounds(lower).upper_bounds(upper);
                files.push(file.build()?);
            }
            Ok(files)
        })))
    }

    pub async fn close(mut self) -> Result<Vec<DataFile>> {
        self.finish_file().await?;
        while let Some(files) = self.closing.next().await {
            self.files.extend(files?);
        }
        if self.closed_concurrently {
            // Uploads finish in any order; artifact names follow write order.
            self.files
                .sort_by(|left, right| left.file_path().cmp(right.file_path()));
        }
        Ok(self.files)
    }
}

/// Whether positions on a target may join the delete file holding the
/// positions of an earlier one, both in one partition; see
/// [`crate::DeleteWriter::with_sharing`].
pub type SharingRule = Arc<dyn Fn(&str, &str) -> bool + Send + Sync>;

/// Delete files a pooled writer keeps open at once, one per partition.
/// Opening another closes the least recently written one.
const MAX_OPEN_DELETE_PARTITIONS: usize = 256;

/// Position deletes in their targets' specs and partitions: one open file
/// per partition, so the sorted positions of an epoch that touches many
/// partitions still pool into one file in each. Input must be sorted by path
/// and position, as for [`ParquetDeleteWriter`].
pub(crate) struct PooledDeleteWriter {
    file_io: FileIO,
    location: String,
    operation: OperationId,
    config: WriterConfig,
    targets: TargetPartitions,
    sharing: Option<SharingRule>,
    next_file: Arc<AtomicUsize>,
    writers: Vec<ParquetDeleteWriter>,
    by_partition: HashMap<(i32, Vec<u8>), usize>,
    last_write: Vec<u64>,
    writes: u64,
    last: Option<usize>,
}

impl PooledDeleteWriter {
    pub(crate) fn new(
        file_io: FileIO,
        location: &str,
        operation: &OperationId,
        targets: TargetPartitions,
        config: WriterConfig,
    ) -> Result<Self> {
        ensure!(
            !operation.0.is_empty()
                && operation
                    .0
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_'),
            "invalid operation ID"
        );
        Ok(Self {
            file_io,
            location: location.to_owned(),
            operation: operation.clone(),
            config,
            targets,
            sharing: None,
            next_file: Arc::new(AtomicUsize::new(0)),
            writers: Vec::new(),
            by_partition: HashMap::new(),
            last_write: Vec::new(),
            writes: 0,
            last: None,
        })
    }

    pub(crate) fn set_sharing(&mut self, rule: SharingRule) {
        self.sharing = Some(rule);
    }

    fn writer(&mut self, path: &str) -> Result<usize> {
        let (spec_id, partition) = self.targets.get(path)?;
        let key = if self.targets.is_uniform() {
            (spec_id, Vec::new())
        } else {
            (spec_id, flow_iceberg_ext::partition_key(partition))
        };
        if let Some(&index) = self.by_partition.get(&key) {
            return Ok(index);
        }
        let partition = (spec_id, partition.clone());
        self.writers.push(ParquetDeleteWriter::new(
            self.file_io.clone(),
            &self.location,
            &self.operation,
            partition,
            self.next_file.clone(),
            self.config.clone(),
        )?);
        self.last_write.push(0);
        self.by_partition.insert(key, self.writers.len() - 1);
        Ok(self.writers.len() - 1)
    }

    pub(crate) async fn write(&mut self, positions: &[impl DeletePosition]) -> Result<()> {
        // Admit the whole call before any partition opens a file, so an
        // oversized late position leaves no partial artifact behind.
        for position in positions {
            ensure!(
                position.row_position() <= i64::MAX as u64,
                "position overflows Iceberg long"
            );
            self.config.admit_row(delete_bytes(position)?)?;
            self.targets.get(position.data_file())?;
        }
        let mut start = 0;
        while start < positions.len() {
            let path = positions[start].data_file();
            let mut end = start + 1;
            while end < positions.len() && positions[end].data_file() == path {
                end += 1;
            }
            let index = self.writer(path)?;
            let writer = &mut self.writers[index];
            if let (Some(rule), Some((previous, _))) = (&self.sharing, &writer.last)
                && writer.current.is_some()
                && previous != path
                && !rule(previous, path)
            {
                writer.finish_file_concurrently().await?;
            }
            if self.writers[index].current.is_none() {
                self.bound_open_files().await?;
            }
            self.writes += 1;
            self.last_write[index] = self.writes;
            self.writers[index].write(&positions[start..end]).await?;
            self.last = Some(index);
            start = end;
        }
        Ok(())
    }

    /// Close the least recently written file once too many are open.
    async fn bound_open_files(&mut self) -> Result<()> {
        let open = self
            .writers
            .iter()
            .filter(|writer| writer.current.is_some())
            .count();
        if open < MAX_OPEN_DELETE_PARTITIONS {
            return Ok(());
        }
        let oldest = (0..self.writers.len())
            .filter(|&index| self.writers[index].current.is_some())
            .min_by_key(|&index| self.last_write[index])
            .expect("open delete files");
        self.writers[oldest].finish_file_concurrently().await
    }

    /// Close every open file before admitting another sorted range.
    pub(crate) async fn finish_file(&mut self) -> Result<()> {
        for writer in &mut self.writers {
            writer.finish_file().await?;
        }
        Ok(())
    }

    /// Close the file the last positions went to, letting its upload overlap
    /// later writes.
    pub(crate) async fn finish_file_concurrently(&mut self) -> Result<()> {
        match self.last {
            Some(index) => self.writers[index].finish_file_concurrently().await,
            None => Ok(()),
        }
    }

    pub(crate) async fn close(self) -> Result<Vec<DataFile>> {
        let mut files = Vec::new();
        for writer in self.writers {
            files.extend(writer.close().await?);
        }
        // Artifact names follow write order across partitions.
        files.sort_by(|left, right| left.file_path().cmp(right.file_path()));
        Ok(files)
    }
}

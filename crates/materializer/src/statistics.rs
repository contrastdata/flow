//! Distinct-value sketches of data files, and the Puffin files that hold them.
//!
//! A data file's sketches are written beside it, as `<data file>.sketch.puffin`,
//! with one `apache-datasketches-theta-v1` blob per sketched field. A table's
//! statistics file unions them under the same blob type. Values are hashed in
//! Iceberg's single-value binary form (Iceberg spec, Appendix D), so the
//! sketches are the ones Spark's and Trino's own statistics would hold.

use std::collections::HashMap;

use anyhow::{Context, Result, ensure};
use arrow_array::{Array, ArrayRef, cast::AsArray, types::*};
use flow_model::{ColumnType, TableSchema};
use iceberg::{
    compression::CompressionCodec,
    io::FileIO,
    puffin::{APACHE_DATASKETCHES_THETA_V1, Blob, CREATED_BY_PROPERTY, PuffinReader, PuffinWriter},
    spec::{BlobMetadata, StatisticsFile},
};

use crate::theta::ThetaSketch;

/// The Puffin file holding the sketches of `data_file`.
pub fn sketch_path(data_file: &str) -> String {
    format!("{data_file}.sketch.puffin")
}

/// The blob property every Theta blob carries: the estimated distinct count.
pub const NDV_PROPERTY: &str = "ndv";

/// The rows a sketch file covers, in its footer properties.
pub const ROWS_PROPERTY: &str = "flow.rows";

/// One sketched column: its position in the writer's schema and its field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SketchColumn {
    pub column: usize,
    pub field_id: i32,
    pub data_type: ColumnType,
}

/// The columns a writer sketches.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SketchSelection {
    pub columns: Vec<SketchColumn>,
}

impl SketchSelection {
    /// Columns of `schema` with the given field IDs, in schema order.
    pub fn of_fields(schema: &TableSchema, field_ids: &[i32]) -> Self {
        Self {
            columns: schema
                .columns
                .iter()
                .enumerate()
                .filter(|(_, column)| field_ids.contains(&column.field_id))
                .map(|(index, column)| SketchColumn {
                    column: index,
                    field_id: column.field_id,
                    data_type: column.data_type.clone(),
                })
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }
}

/// Sketches of one data file's rows.
#[derive(Debug, Clone)]
pub struct FileSketches {
    pub sketches: Vec<(i32, ThetaSketch)>,
    pub rows: u64,
}

impl FileSketches {
    pub fn new(selection: &SketchSelection) -> Self {
        Self {
            sketches: selection
                .columns
                .iter()
                .map(|column| (column.field_id, ThetaSketch::new()))
                .collect(),
            rows: 0,
        }
    }

    /// Offer a batch in the writer's column order.
    pub fn update(&mut self, selection: &SketchSelection, columns: &[ArrayRef]) -> Result<()> {
        let rows = columns.first().map_or(0, |column| column.len());
        for (column, (_, sketch)) in selection.columns.iter().zip(&mut self.sketches) {
            let array = columns
                .get(column.column)
                .context("sketched column is outside the batch")?;
            update_sketch(sketch, array.as_ref(), &column.data_type)?;
        }
        self.rows += rows as u64;
        Ok(())
    }
}

/// The minimal big-endian two's-complement bytes of a decimal's unscaled
/// value, as Iceberg serializes decimals.
pub fn decimal_bytes(unscaled: i128) -> Vec<u8> {
    let bytes = unscaled.to_be_bytes();
    let mut start = 0;
    while start < bytes.len() - 1 {
        let redundant = (bytes[start] == 0x00 && bytes[start + 1] & 0x80 == 0)
            || (bytes[start] == 0xff && bytes[start + 1] & 0x80 != 0);
        if !redundant {
            break;
        }
        start += 1;
    }
    bytes[start..].to_vec()
}

/// Offer every non-null value of `array`, serialized as Iceberg's
/// single-value binary form of `data_type`.
pub fn update_sketch(
    sketch: &mut ThetaSketch,
    array: &dyn Array,
    data_type: &ColumnType,
) -> Result<()> {
    macro_rules! primitive {
        ($kind:ty) => {{
            let values = array
                .as_primitive_opt::<$kind>()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(&value.to_le_bytes());
            }
        }};
    }
    match data_type {
        ColumnType::Bool => {
            let values = array
                .as_boolean_opt()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(&[u8::from(value)]);
            }
        }
        ColumnType::Int32 => primitive!(Int32Type),
        ColumnType::Date => primitive!(Date32Type),
        ColumnType::Int64 => primitive!(Int64Type),
        ColumnType::TimestampMicros | ColumnType::TimestampTzMicros => {
            primitive!(TimestampMicrosecondType)
        }
        ColumnType::Float64 => {
            let values = array
                .as_primitive_opt::<Float64Type>()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(&value.to_bits().to_le_bytes());
            }
        }
        ColumnType::String => {
            let values = array
                .as_string_opt::<i32>()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(value.as_bytes());
            }
        }
        ColumnType::Binary => {
            if let Some(values) = array.as_binary_opt::<i64>() {
                for value in values.iter().flatten() {
                    sketch.update(value);
                }
            } else {
                let values = array
                    .as_binary_opt::<i32>()
                    .context("sketched column has an unexpected Arrow type")?;
                for value in values.iter().flatten() {
                    sketch.update(value);
                }
            }
        }
        ColumnType::Uuid => {
            let values = array
                .as_fixed_size_binary_opt()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(value);
            }
        }
        ColumnType::Decimal { .. } => {
            let values = array
                .as_primitive_opt::<Decimal128Type>()
                .context("sketched column has an unexpected Arrow type")?;
            for value in values.iter().flatten() {
                sketch.update(&decimal_bytes(value));
            }
        }
    }
    Ok(())
}

fn blob(field_id: i32, sketch: &mut ThetaSketch, snapshot: (i64, i64)) -> Blob {
    Blob::builder()
        .r#type(APACHE_DATASKETCHES_THETA_V1.to_owned())
        .fields(vec![field_id])
        .snapshot_id(snapshot.0)
        .sequence_number(snapshot.1)
        .data(sketch.serialize())
        .properties(HashMap::from([(
            NDV_PROPERTY.to_owned(),
            sketch.ndv().to_string(),
        )]))
        .build()
}

/// Write `sketches` as one Theta blob per field. `snapshot` is the snapshot
/// ID and sequence number the blobs describe, `(-1, -1)` for a data file's
/// own sketches. Without `replace`, an existing file at `path` is an error.
/// Returns the file's size and footer size.
pub async fn write_sketches(
    file_io: &FileIO,
    path: &str,
    sketches: &mut FileSketches,
    snapshot: (i64, i64),
    replace: bool,
) -> Result<(u64, u64)> {
    let output = file_io.new_output(path)?;
    let output = if replace { output } else { output.if_absent() };
    let mut writer = PuffinWriter::new(
        &output,
        HashMap::from([
            (CREATED_BY_PROPERTY.to_owned(), "contrast-flow".to_owned()),
            (ROWS_PROPERTY.to_owned(), sketches.rows.to_string()),
        ]),
        false,
    )
    .await?;
    let mut blobs_end = 4u64;
    for (field_id, sketch) in &mut sketches.sketches {
        let blob = blob(*field_id, sketch, snapshot);
        blobs_end += blob.data().len() as u64;
        writer.add(blob, CompressionCodec::None).await?;
    }
    writer.close().await?;
    let size = file_io.new_input(path)?.metadata().await?.size;
    ensure!(
        size > blobs_end,
        "statistics file is shorter than its blobs"
    );
    Ok((size, size - blobs_end))
}

/// Read the sketches of a Puffin file Flow wrote, by field.
pub async fn read_sketches(file_io: &FileIO, path: &str) -> Result<FileSketches> {
    let reader = PuffinReader::new(file_io.new_input(path)?);
    let metadata = reader.file_metadata().await?.clone();
    let rows = metadata
        .properties()
        .get(ROWS_PROPERTY)
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(0);
    let mut sketches = Vec::new();
    for blob in metadata.blobs() {
        if blob.blob_type() != APACHE_DATASKETCHES_THETA_V1 || blob.fields().len() != 1 {
            continue;
        }
        let data = reader.blob(blob).await?;
        sketches.push((blob.fields()[0], ThetaSketch::deserialize(data.data())?));
    }
    Ok(FileSketches { sketches, rows })
}

/// The table-metadata entry for a statistics file written by
/// [`write_sketches`] for `snapshot`.
pub fn statistics_file(
    path: &str,
    sizes: (u64, u64),
    sketches: &mut FileSketches,
    snapshot: (i64, i64),
) -> Result<StatisticsFile> {
    Ok(StatisticsFile {
        snapshot_id: snapshot.0,
        statistics_path: path.to_owned(),
        file_size_in_bytes: i64::try_from(sizes.0)?,
        file_footer_size_in_bytes: i64::try_from(sizes.1)?,
        key_metadata: None,
        blob_metadata: sketches
            .sketches
            .iter_mut()
            .map(|(field_id, sketch)| BlobMetadata {
                r#type: APACHE_DATASKETCHES_THETA_V1.to_owned(),
                snapshot_id: snapshot.0,
                sequence_number: snapshot.1,
                fields: vec![*field_id],
                properties: HashMap::from([(NDV_PROPERTY.to_owned(), sketch.ndv().to_string())]),
            })
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimals_use_the_fewest_twos_complement_bytes() {
        for (value, bytes) in [
            (0i128, &[0x00][..]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x00, 0x80]),
            (-1, &[0xff]),
            (-128, &[0x80]),
            (-129, &[0xff, 0x7f]),
            (65535, &[0x00, 0xff, 0xff]),
        ] {
            assert_eq!(decimal_bytes(value), bytes, "{value}");
        }
    }
}

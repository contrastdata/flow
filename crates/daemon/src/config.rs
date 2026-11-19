use anyhow::{Context, Result, bail, ensure};
use flow_coordinator::{AckMode, JournalDurability, Priority};
use flow_ingress_journal::JournalConfig;
use flow_model::{Column, TableId, TableSchema};
use flow_pg_source::SpoolConfig;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// A relative path is resolved against the configuration file's directory.
    pub state_dir: PathBuf,
    pub source: Source,
    pub catalog: HashMap<String, String>,
    /// Required except by `discover`, which generates these blocks.
    #[serde(default)]
    pub tables: Vec<Table>,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub compaction: flow_compactor::Policy,
    #[serde(default)]
    pub parquet_read: flow_compactor::ReadLimits,
    /// Distinct-value statistics of target tables; off unless enabled.
    #[serde(default)]
    pub statistics: Statistics,
    /// Optional HTTP endpoints for probes and Prometheus scraping.
    #[serde(default)]
    pub http: Option<Http>,
    /// Local state volume safety.
    #[serde(default)]
    pub storage: Storage,
    /// Replicate the journal and control state to object storage, so a worker
    /// and its local disk can be replaced without losing acknowledged changes.
    #[serde(default)]
    pub durability: Option<flow_durability::Config>,
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Storage {
    /// Capture pauses while the state volume has fewer free bytes, and resumes
    /// a quarter above it. Zero disables the watermark.
    pub min_free_bytes: u64,
    /// Verify every index checksum on each start. Otherwise the full scan runs
    /// only after an unclean exit or `check --storage`.
    pub verify_index_on_start: bool,
    /// What backs `state_dir`. `memory` declares a tmpfs or ramfs mount.
    pub medium: Medium,
}
impl Default for Storage {
    fn default() -> Self {
        Self {
            min_free_bytes: 4 << 30,
            verify_index_on_start: false,
            medium: Medium::Disk,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Medium {
    #[default]
    Disk,
    /// A memory-backed filesystem: free space is also bounded by the memory
    /// left to this process, and the contents do not survive a reboot, so
    /// object storage must be the authority.
    Memory,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Http {
    /// For example `0.0.0.0:9464`; serves /healthz, /readyz and /metrics.
    pub listen: std::net::SocketAddr,
    /// /healthz fails once the running service's main loop has not advanced
    /// for this long; zero disables the check. Startup, index rebuild and
    /// initial COPY are never judged.
    #[serde(default = "default_liveness_timeout_secs")]
    pub liveness_timeout_secs: u64,
}
fn default_liveness_timeout_secs() -> u64 {
    300
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Resolve the URL at runtime so checked-in configuration contains no secrets.
    pub connection_env: String,
    pub id: String,
    pub slot: String,
    pub publication: String,
    #[serde(default)]
    pub ack_mode: AckMode,
    #[serde(default)]
    pub journal_durability: JournalDurability,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Table {
    /// Explicit selection freezes configured columns; all_current allows safe nullable additions.
    #[serde(default)]
    pub column_selection: ColumnSelection,
    pub source_namespace: String,
    pub source_table: String,
    pub target_namespace: Vec<String>,
    pub target_table: String,
    /// Format for newly created targets; existing targets retain their format.
    #[serde(default = "default_format_version")]
    pub format_version: iceberg::spec::FormatVersion,
    pub columns: Vec<Column>,
    pub primary_key: Vec<usize>,
    #[serde(default)]
    pub append_only: bool,
    #[serde(default)]
    pub priority: Priority,
    /// The target's Iceberg sort order. Absent: a table Flow creates is
    /// sorted by its primary key and an existing table keeps its own order.
    /// Set: Flow also replaces a differing order on an existing table.
    #[serde(default)]
    pub sort: Option<SortSetting>,
    /// The target's Iceberg partition spec: `"none"` or a list of fields such
    /// as `"day(created_at)"`, `"bucket(16, id)"` or `"region"`. Absent: a
    /// table Flow creates is unpartitioned and an existing table keeps its
    /// own spec. Set: Flow also replaces a differing spec on an existing table.
    #[serde(default)]
    pub partition: Option<PartitionSetting>,
    /// How files written in an earlier spec reach the current one.
    #[serde(default)]
    pub partition_convert: PartitionConvert,
    /// Columns whose distinct values are sketched when `[statistics]` is
    /// enabled: `"auto"`, `"none"` or a list of column names. Absent:
    /// `[statistics] columns`.
    #[serde(default)]
    pub statistics: Option<StatisticsColumns>,
}

/// `[statistics]`: Theta sketches of distinct values, published as Iceberg
/// statistics files readers such as Trino use for planning. Off by default.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Statistics {
    pub enabled: bool,
    /// Columns of tables without their own `statistics` setting.
    pub columns: StatisticsColumns,
    /// Most columns `"auto"` selects per table.
    pub max_columns: usize,
    /// Least time between two publications of one table's statistics.
    pub refresh_interval_secs: u64,
    /// Publish only when sketched data files hold this share of the rows.
    pub min_coverage_percent: u8,
    /// Rebuild a table's sketches once rewritten files held this share.
    pub recompute_percent: u8,
    /// Sketch files one visit reads.
    pub max_sketch_reads: usize,
    /// Unsketched data files one visit reads to sketch.
    pub backfill_files: usize,
}

impl Default for Statistics {
    fn default() -> Self {
        let policy = flow_coordinator::StatisticsPolicy::default();
        Self {
            enabled: false,
            columns: StatisticsColumns::Auto,
            max_columns: 16,
            refresh_interval_secs: policy.refresh_interval.as_secs(),
            min_coverage_percent: policy.min_coverage_percent,
            recompute_percent: policy.recompute_percent,
            max_sketch_reads: policy.max_sketch_reads,
            backfill_files: policy.backfill_files,
        }
    }
}

impl Statistics {
    pub fn policy(&self) -> flow_coordinator::StatisticsPolicy {
        flow_coordinator::StatisticsPolicy {
            refresh_interval: std::time::Duration::from_secs(self.refresh_interval_secs),
            min_coverage_percent: self.min_coverage_percent,
            recompute_percent: self.recompute_percent,
            max_sketch_reads: self.max_sketch_reads,
            backfill_files: self.backfill_files,
            ..Default::default()
        }
    }
}

/// Columns to sketch: `"auto"`, `"none"`, or column names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatisticsColumns {
    /// Primary-key columns, then integer, date, timestamp, UUID, decimal and
    /// string columns in schema order, up to `max_columns`.
    Auto,
    None,
    Columns(Vec<String>),
}

impl<'de> Deserialize<'de> for StatisticsColumns {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Name(String),
            Columns(Vec<String>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Name(name) if name == "auto" => Ok(Self::Auto),
            Raw::Name(name) if name == "none" => Ok(Self::None),
            Raw::Name(_) => Err(serde::de::Error::custom(
                "statistics must be \"auto\", \"none\" or a list of columns",
            )),
            Raw::Columns(columns) => Ok(Self::Columns(columns)),
        }
    }
}

/// A configured partition spec: `"none"`, or a list of partition fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PartitionSetting {
    Unpartitioned,
    Fields(Vec<String>),
}

impl<'de> Deserialize<'de> for PartitionSetting {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Name(String),
            Fields(Vec<String>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Name(name) if name == "none" => Ok(Self::Unpartitioned),
            Raw::Name(_) => Err(serde::de::Error::custom(
                "partition must be \"none\" or a list of fields",
            )),
            Raw::Fields(fields) => Ok(Self::Fields(fields)),
        }
    }
}

/// How data files written in an earlier partition spec reach the current
/// one. Either way, new data and every rewrite use the current spec.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionConvert {
    /// Only when compaction rewrites them for its own reasons.
    #[default]
    Lazy,
    /// Also by background rewrites, within
    /// `[compaction] partition_convert_bytes_per_hour`.
    Background,
}

/// A configured sort order: `"primary_key"`, `"none"`, or a list of
/// `"column [asc|desc] [nulls first|nulls last]"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SortSetting {
    PrimaryKey,
    Unsorted,
    Columns(Vec<String>),
}

impl<'de> Deserialize<'de> for SortSetting {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Raw {
            Name(String),
            Columns(Vec<String>),
        }
        match Raw::deserialize(deserializer)? {
            Raw::Name(name) if name == "primary_key" => Ok(Self::PrimaryKey),
            Raw::Name(name) if name == "none" => Ok(Self::Unsorted),
            Raw::Name(_) => Err(serde::de::Error::custom(
                "sort must be \"primary_key\", \"none\" or a list of columns",
            )),
            Raw::Columns(columns) => Ok(Self::Columns(columns)),
        }
    }
}

#[derive(Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnSelection {
    #[default]
    AllCurrent,
    Explicit,
}

fn default_format_version() -> iceberg::spec::FormatVersion {
    iceberg::spec::FormatVersion::V2
}

/// Parse one partition field: `column`, `identity(column)`, `year(column)`,
/// `month(column)`, `day(column)`, `hour(column)`, `bucket(N, column)` or
/// `truncate(W, column)`.
fn partition_field(field: &str) -> Result<(iceberg::spec::Transform, &str)> {
    use iceberg::spec::Transform;
    let field = field.trim();
    let Some((name, rest)) = field.split_once('(') else {
        ensure!(
            !field.is_empty() && !field.contains([')', ',', ' ']),
            "partition field {field:?} must be a column or transform(column)"
        );
        return Ok((Transform::Identity, field));
    };
    let arguments = rest
        .strip_suffix(')')
        .with_context(|| format!("partition field {field:?} lacks a closing parenthesis"))?;
    let arguments: Vec<&str> = arguments.split(',').map(str::trim).collect();
    let width = |value: &str| {
        value
            .parse::<u32>()
            .ok()
            .filter(|width| *width > 0)
            .with_context(|| format!("partition field {field:?} needs a positive width"))
    };
    let (transform, column) = match (name.trim().to_lowercase().as_str(), &arguments[..]) {
        ("identity", [column]) => (Transform::Identity, *column),
        ("year" | "years", [column]) => (Transform::Year, *column),
        ("month" | "months", [column]) => (Transform::Month, *column),
        ("day" | "days", [column]) => (Transform::Day, *column),
        ("hour" | "hours", [column]) => (Transform::Hour, *column),
        ("bucket", [buckets, column]) => (Transform::Bucket(width(buckets)?), *column),
        ("truncate", [width_text, column]) => (Transform::Truncate(width(width_text)?), *column),
        _ => bail!(
            "partition field {field:?} must be column, identity, year, month, day, hour, \
             bucket(N, column) or truncate(W, column)"
        ),
    };
    ensure!(
        !column.is_empty() && !column.contains(' '),
        "partition field {field:?} names no column"
    );
    Ok((transform, column))
}

/// Iceberg's conventional partition field names, as Spark and Java assign.
fn partition_field_name(column: &str, transform: &iceberg::spec::Transform) -> String {
    use iceberg::spec::Transform;
    match transform {
        Transform::Identity => column.to_owned(),
        Transform::Bucket(_) => format!("{column}_bucket"),
        Transform::Truncate(_) => format!("{column}_trunc"),
        Transform::Year => format!("{column}_year"),
        Transform::Month => format!("{column}_month"),
        Transform::Day => format!("{column}_day"),
        Transform::Hour => format!("{column}_hour"),
        other => format!("{column}_{other}"),
    }
}

impl Table {
    /// The columns this table sketches under `statistics`, or `None` when
    /// statistics are off for it.
    pub fn sketch_selection(
        &self,
        statistics: &Statistics,
        schema: &TableSchema,
    ) -> Result<Option<flow_materializer::SketchSelection>> {
        let setting = self.statistics.as_ref().unwrap_or(&statistics.columns);
        let fields: Vec<i32> = match setting {
            StatisticsColumns::None => return Ok(None),
            StatisticsColumns::Columns(names) => {
                ensure!(
                    !names.is_empty(),
                    "statistics lists no columns; use \"none\""
                );
                let mut fields = Vec::new();
                for name in names {
                    let column = schema
                        .columns
                        .iter()
                        .find(|column| &column.name == name)
                        .with_context(|| {
                            format!("statistics column {name:?} is not a configured column")
                        })?;
                    ensure!(
                        !fields.contains(&column.field_id),
                        "statistics lists column {name:?} twice"
                    );
                    fields.push(column.field_id);
                }
                fields
            }
            StatisticsColumns::Auto => {
                use flow_model::ColumnType;
                let mut fields: Vec<i32> = schema
                    .primary_key
                    .iter()
                    .map(|&index| schema.columns[index].field_id)
                    .collect();
                for column in &schema.columns {
                    let eligible = matches!(
                        column.data_type,
                        ColumnType::Int32
                            | ColumnType::Int64
                            | ColumnType::Date
                            | ColumnType::TimestampMicros
                            | ColumnType::TimestampTzMicros
                            | ColumnType::Uuid
                            | ColumnType::Decimal { .. }
                            | ColumnType::String
                    );
                    if eligible && !fields.contains(&column.field_id) {
                        fields.push(column.field_id);
                    }
                }
                fields.truncate(statistics.max_columns);
                fields
            }
        };
        if !statistics.enabled || fields.is_empty() {
            return Ok(None);
        }
        Ok(Some(flow_materializer::SketchSelection::of_fields(
            schema, &fields,
        )))
    }

    /// The sort keys Flow declares on this target: the configured order, or
    /// the primary key when none is configured. Keyless tables are unsorted.
    pub fn sort_keys(&self) -> Result<Vec<flow_materializer::SortKey>> {
        let primary_key = || flow_materializer::primary_key_sort(&self.schema(0));
        match &self.sort {
            None => Ok(primary_key()),
            Some(SortSetting::PrimaryKey) => {
                ensure!(
                    !self.primary_key.is_empty(),
                    "sort = \"primary_key\" requires a primary key"
                );
                Ok(primary_key())
            }
            Some(SortSetting::Unsorted) => Ok(Vec::new()),
            Some(SortSetting::Columns(fields)) => {
                ensure!(!fields.is_empty(), "sort lists no columns; use \"none\"");
                let mut keys: Vec<flow_materializer::SortKey> = Vec::with_capacity(fields.len());
                for field in fields {
                    let key = self.sort_field(field)?;
                    ensure!(
                        keys.iter().all(|existing| existing.column != key.column),
                        "sort lists column {field:?} twice"
                    );
                    keys.push(key);
                }
                Ok(keys)
            }
        }
    }

    /// The partition spec Flow declares on this target, bound to `schema`:
    /// `None` when unconfigured, an unpartitioned spec for `"none"`.
    pub fn partition_spec(
        &self,
        schema: &iceberg::spec::Schema,
    ) -> Result<Option<iceberg::spec::PartitionSpec>> {
        let fields = match &self.partition {
            None => return Ok(None),
            Some(PartitionSetting::Unpartitioned) => &[][..],
            Some(PartitionSetting::Fields(fields)) => {
                ensure!(
                    !fields.is_empty(),
                    "partition lists no fields; use \"none\""
                );
                &fields[..]
            }
        };
        let mut builder = iceberg::spec::PartitionSpec::builder(schema.clone());
        for field in fields {
            let (transform, column) = partition_field(field)?;
            ensure!(
                self.columns
                    .iter()
                    .any(|configured| configured.name == column),
                "partition column {column:?} is not a configured column"
            );
            builder = builder
                .add_partition_field(column, partition_field_name(column, &transform), transform)
                .with_context(|| format!("invalid partition field {field:?}"))?;
        }
        Ok(Some(builder.build().context("invalid partition spec")?))
    }

    /// Parse `column [asc|desc] [nulls first|nulls last]`. As in Iceberg and
    /// Spark, nulls default to first for ascending and last for descending.
    fn sort_field(&self, field: &str) -> Result<flow_materializer::SortKey> {
        let words: Vec<String> = field.split_whitespace().map(str::to_lowercase).collect();
        let name = field.split_whitespace().next().unwrap_or_default();
        let column = self
            .columns
            .iter()
            .position(|column| column.name == name)
            .with_context(|| format!("sort column {name:?} is not a configured column"))?;
        let (descending, rest) = match words.get(1).map(String::as_str) {
            Some("asc") => (false, &words[2..]),
            Some("desc") => (true, &words[2..]),
            _ => (false, &words[1..]),
        };
        let nulls_first = match rest.iter().map(String::as_str).collect::<Vec<_>>()[..] {
            [] => !descending,
            ["nulls", "first"] => true,
            ["nulls", "last"] => false,
            _ => {
                bail!("sort field {field:?} must be \"column [asc|desc] [nulls first|nulls last]\"")
            }
        };
        Ok(flow_materializer::SortKey {
            column,
            descending,
            nulls_first,
        })
    }

    pub fn projection(&self) -> Option<Vec<String>> {
        (self.column_selection == ColumnSelection::Explicit).then(|| {
            self.columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        })
    }
    pub fn schema(&self, id: u32) -> TableSchema {
        TableSchema {
            table_id: TableId(id),
            version: 0,
            columns: self.columns.clone(),
            primary_key: self.primary_key.clone(),
            append_only: self.append_only,
        }
    }
}
#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Limits {
    pub journal_bytes: u64,
    pub spool_bytes: u64,
    /// Source transactions open in the capture spool at once, including
    /// streamed in-progress transactions.
    pub spool_transactions: usize,
    /// Savepoints with captured rows per open source transaction.
    pub spool_subtransactions: usize,
    pub chunk_bytes: u32,
    /// A pgoutput UPDATE includes both row images and text bytea expansion.
    pub source_message_bytes: usize,
    pub batch_rows: usize,
    pub batch_bytes: usize,
    /// Additional retained collapse state per table worker; zero uses disk only.
    pub collapse_memory_bytes: usize,
    /// Fold a busy table's next epoch from the journal while its current
    /// epoch commits and applies, within the same collapse budget.
    pub collapse_prefold: bool,
    pub parquet_row_group_bytes: usize,
    /// Threads that may encode the columns of Parquet batches at once across
    /// the process, each writer's own thread included. A large batch whose
    /// encoding is spread across several fields uses idle ones; others stay
    /// on the writer's thread. Unset: half the cores, at most four. `0` or `1`
    /// keeps every batch on its writer's thread.
    pub parquet_encode_threads: Option<usize>,
    /// Rows per Parquet row group in compacted files and the initial copy,
    /// which readers scan most. Publication files keep 8192 rows, so CDC
    /// epochs do not buffer larger groups.
    pub parquet_row_group_rows: usize,
    /// Queued lookahead descriptors shared by all tables. It makes tables
    /// schedulable; a dispatched epoch continues from the durable ledger up to
    /// its payload and descriptor-memory limits, so this does not cap batches.
    pub pending_transactions: usize,
    /// Optional cap on source transactions in one publication epoch, bounding
    /// per-epoch commit work; unset, payload and memory limits size epochs.
    pub epoch_max_transactions: Option<usize>,
    pub table_workers: usize,
    pub commits_per_second: u32,
    pub wal_soft_bytes: u64,
    pub wal_hard_bytes: u64,
    pub snapshot_retention_secs: u64,
    /// Without expiration, table metadata and replaced files grow without bound.
    /// Disable only when another coordinated process expires snapshots.
    pub snapshot_expiration: bool,
    /// Expiration never leaves fewer snapshots than this on `main`.
    pub snapshot_retain_last: usize,
    /// Expiration removes the oldest unprotected snapshots beyond this count,
    /// even inside the retention window. Each commit rewrites all retained history.
    pub snapshot_max_count: usize,
    pub checkpoint_interval_secs: u64,
    pub retained_checkpoints: usize,
    pub manifest_max_count: usize,
    pub garbage_interval_secs: u64,
    pub orphan_grace_secs: u64,
    /// Grace for superseded catalog metadata JSON once it leaves the catalog
    /// metadata log. Defaults to the smaller of 3600 and `orphan_grace_secs`.
    pub metadata_json_grace_secs: Option<u64>,
    /// How long another writer's row changes to a target hold that table's
    /// source WAL while publication retries. Rolling back the foreign snapshot
    /// within it resumes the table; afterwards only a resync recovers it.
    pub external_mutation_grace_secs: u64,
}
pub(crate) const DEFAULT_EXTERNAL_MUTATION_GRACE_SECS: u64 = 900;
impl Limits {
    pub fn metadata_json_grace_secs(&self) -> u64 {
        self.metadata_json_grace_secs
            .unwrap_or(self.orphan_grace_secs.min(3600))
    }
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            journal_bytes: 64 << 30,
            spool_bytes: 32 << 30,
            spool_transactions: SpoolConfig::default().max_transactions,
            spool_subtransactions: SpoolConfig::default().max_subtransactions,
            chunk_bytes: 4 << 20,
            source_message_bytes: 32 << 20,
            batch_rows: 1024,
            batch_bytes: 8 << 20,
            collapse_memory_bytes: 128 << 20,
            collapse_prefold: true,
            parquet_row_group_bytes: 32 << 20,
            parquet_encode_threads: None,
            parquet_row_group_rows: 122_880,
            pending_transactions: 256,
            epoch_max_transactions: None,
            table_workers: 4,
            commits_per_second: 100,
            wal_soft_bytes: 16 << 30,
            wal_hard_bytes: 32 << 30,
            snapshot_retention_secs: 3600,
            snapshot_expiration: true,
            snapshot_retain_last: 128,
            snapshot_max_count: 1000,
            checkpoint_interval_secs: 300,
            retained_checkpoints: 2,
            manifest_max_count: 64,
            garbage_interval_secs: 300,
            orphan_grace_secs: 86400,
            metadata_json_grace_secs: None,
            external_mutation_grace_secs: DEFAULT_EXTERNAL_MUTATION_GRACE_SECS,
        }
    }
}
/// Anchor a relative `state_dir` to the configuration file, not the working
/// directory, so every command and supervisor finds the same state.
fn resolve_state_dir(config: &std::path::Path, state_dir: &std::path::Path) -> Result<PathBuf> {
    if state_dir.is_absolute() {
        return Ok(state_dir.to_owned());
    }
    let config = std::path::absolute(config).context("resolve configuration path")?;
    let directory = config
        .parent()
        .context("configuration path has no parent directory")?;
    let relative = state_dir
        .components()
        .filter(|component| !matches!(component, std::path::Component::CurDir))
        .collect::<PathBuf>();
    Ok(if relative.as_os_str().is_empty() {
        directory.to_owned()
    } else {
        directory.join(relative)
    })
}
impl Limits {
    pub fn history_policy(&self) -> flow_coordinator::HistoryPolicy {
        flow_coordinator::HistoryPolicy {
            retention: std::time::Duration::from_secs(self.snapshot_retention_secs),
            retain_last: self.snapshot_retain_last,
            max_snapshots: self.snapshot_max_count,
        }
    }
}
impl Config {
    pub fn load(path: &std::path::Path) -> Result<Self> {
        let config = Self::load_without_tables(path)?;
        config.validate()?;
        Ok(config)
    }

    /// Validate everything except the table list, for `discover`.
    pub fn load_without_tables(path: &std::path::Path) -> Result<Self> {
        let input = std::fs::read_to_string(path).context("read configuration")?;
        let mut config: Self = toml::from_str(&input).map_err(|error: toml::de::Error| {
            // Both source excerpts and serde messages can contain secret values.
            // Retain the location without chaining the original error.
            let offset = error.span().map_or(0, |span| span.start).min(input.len());
            let prefix = &input[..input.floor_char_boundary(offset)];
            let line = prefix.bytes().filter(|byte| *byte == b'\n').count() + 1;
            let column = prefix
                .rsplit('\n')
                .next()
                .unwrap_or_default()
                .chars()
                .count()
                + 1;
            anyhow::anyhow!("invalid configuration at line {line}, column {column}")
        })?;
        config.validate_source()?;
        config.state_dir = resolve_state_dir(path, &config.state_dir)?;
        // Like state_dir, a relative CA bundle belongs to the configuration.
        if let Some(file) = config.catalog.get_mut("tls_ca_file") {
            *file = resolve_state_dir(path, std::path::Path::new(file.as_str()))?
                .to_string_lossy()
                .into_owned();
        }
        Ok(config)
    }

    /// Resolve catalog secrets only when connecting, not during `check` or `status`.
    pub fn catalog_properties(&self) -> Result<HashMap<String, String>> {
        self.resolve_catalog_properties(|name| std::env::var(name).ok())
    }

    fn resolve_catalog_properties(
        &self,
        lookup: impl Fn(&str) -> Option<String>,
    ) -> Result<HashMap<String, String>> {
        let mut properties = self.catalog.clone();
        for key in ["token", "credential"] {
            if let Some(name) = properties.remove(&format!("{key}_env")) {
                // EnvVarError::NotUnicode can include the secret in its Debug output.
                let value = lookup(&name).ok_or_else(|| {
                    crate::exit::config(format!(
                        "catalog.{key}_env must name a set, Unicode environment variable"
                    ))
                })?;
                ensure!(
                    !value.is_empty(),
                    crate::exit::config(format!("catalog.{key}_env resolved to an empty value"))
                );
                properties.insert(key.to_owned(), value);
            }
        }
        Ok(properties)
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(!self.tables.is_empty(), "source and tables are required");
        let statistics = &self.statistics;
        ensure!(
            statistics.max_columns > 0
                && statistics.refresh_interval_secs > 0
                && statistics.min_coverage_percent <= 100
                && (1..=100).contains(&statistics.recompute_percent)
                && statistics.max_sketch_reads > 0,
            "invalid [statistics] limits"
        );
        self.validate_source()
    }

    /// Replicated state is independent of this machine's disk by construction.
    pub fn journal_durability(&self) -> JournalDurability {
        if self.durability.is_some() {
            JournalDurability::IndependentStorage
        } else {
            self.source.journal_durability
        }
    }

    fn validate_source(&self) -> Result<()> {
        ensure!(
            !self.source.id.is_empty()
                && !self.source.slot.is_empty()
                && !self.source.publication.is_empty(),
            "source and tables are required"
        );
        ensure!(
            self.source.ack_mode != AckMode::Journaled
                || self.journal_durability() == JournalDurability::IndependentStorage,
            "journaled acknowledgement requires independently durable storage"
        );
        if let Some(durability) = &self.durability {
            durability.validate()?;
        }
        ensure!(
            self.storage.medium != Medium::Memory || self.durability.is_some(),
            "storage.medium = \"memory\" requires [durability]: a memory-backed state_dir does not survive a reboot"
        );
        ensure!(self.catalog.contains_key("uri"), "catalog.uri is required");
        for key in ["token", "credential"] {
            if let Some(name) = self.catalog.get(&format!("{key}_env")) {
                ensure!(
                    !self.catalog.contains_key(key),
                    "configure only one of catalog.{key} and catalog.{key}_env"
                );
                ensure!(
                    !name.is_empty() && !name.contains(['=', '\0']),
                    "catalog.{key}_env must be an environment variable name"
                );
            }
        }
        self.compaction.validate()?;
        self.parquet_read.validate()?;
        let l = &self.limits;
        JournalConfig {
            quota_bytes: l.journal_bytes,
            max_frame_bytes: l.chunk_bytes,
            ..JournalConfig::default()
        }
        .validate()
        .context("invalid journal limits")?;
        SpoolConfig {
            quota_bytes: l.spool_bytes,
            max_chunk_bytes: l.chunk_bytes,
            max_transactions: l.spool_transactions,
            max_subtransactions: l.spool_subtransactions,
            ..SpoolConfig::default()
        }
        .validate()
        .context("invalid spool limits")?;
        ensure!(
            l.chunk_bytes >= 64
                && l.source_message_bytes >= l.chunk_bytes as usize
                && l.batch_rows >= 2
                && l.batch_bytes >= l.chunk_bytes as usize
                && l.parquet_row_group_bytes >= l.batch_bytes
                && l.parquet_row_group_rows > 0
                && l.batch_bytes <= i32::MAX as usize
                && l.pending_transactions > 0
                && l.epoch_max_transactions != Some(0)
                && l.table_workers > 0
                && l.commits_per_second > 0,
            "invalid batching limits"
        );
        ensure!(
            l.wal_soft_bytes > 0 && l.wal_soft_bytes < l.wal_hard_bytes,
            "WAL soft threshold must be below hard threshold"
        );
        ensure!(
            l.snapshot_retention_secs > 0
                && l.checkpoint_interval_secs > 0
                && l.retained_checkpoints > 0
                && l.manifest_max_count >= 2,
            "invalid history, checkpoint, or manifest maintenance limits"
        );
        l.history_policy()
            .validate()
            .context("invalid snapshot history limits")?;
        ensure!(
            l.garbage_interval_secs > 0
                && l.orphan_grace_secs > 0
                && l.metadata_json_grace_secs != Some(0),
            "garbage interval and orphan grace must be positive"
        );
        let mut sources = BTreeMap::new();
        let mut targets = BTreeMap::new();
        for table in &self.tables {
            ensure!(
                matches!(
                    table.format_version,
                    iceberg::spec::FormatVersion::V2 | iceberg::spec::FormatVersion::V3
                ),
                "target format_version must be 2 or 3"
            );
            ensure!(
                !table.source_namespace.is_empty()
                    && !table.source_table.is_empty()
                    && !table.target_namespace.is_empty()
                    && !table.target_table.is_empty(),
                "table names are required"
            );
            ensure!(
                sources
                    .insert((&table.source_namespace, &table.source_table), ())
                    .is_none(),
                "duplicate source table"
            );
            ensure!(
                targets
                    .insert((&table.target_namespace, &table.target_table), ())
                    .is_none(),
                "duplicate target table"
            );
            table.schema(0).validate()?;
            table
                .sort_keys()
                .with_context(|| format!("invalid sort for target table {}", table.target_table))?;
            table
                .partition_spec(&flow_materializer::iceberg_schema(&table.schema(0))?)
                .with_context(|| {
                    format!("invalid partition for target table {}", table.target_table)
                })?;
            table
                .sketch_selection(&self.statistics, &table.schema(0))
                .with_context(|| {
                    format!("invalid statistics for target table {}", table.target_table)
                })?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn relative_state_dir_resolves_against_the_configuration_directory() {
        let root = tempfile::tempdir().unwrap();
        let example = include_str!("../../../examples/flow.toml");
        assert!(example.contains("\nstate_dir = \"./data\"\n"));
        let path = root.path().join("flow.toml");
        std::fs::write(&path, example).unwrap();
        let root = std::path::absolute(root.path()).unwrap();
        assert_eq!(Config::load(&path).unwrap().state_dir, root.join("data"));
        std::fs::write(&path, example.replacen("./data", "..", 1)).unwrap();
        assert_eq!(Config::load(&path).unwrap().state_dir, root.join(".."));
        std::fs::write(&path, example.replacen("./data", ".", 1)).unwrap();
        assert_eq!(Config::load(&path).unwrap().state_dir, root);
        std::fs::write(&path, example.replacen("./data", "/srv/flow", 1)).unwrap();
        assert_eq!(
            Config::load(&path).unwrap().state_dir,
            PathBuf::from("/srv/flow")
        );
        let with_ca = |value: &str| {
            example.replacen(
                "[catalog]\n",
                &format!("[catalog]\ntls_ca_file = \"{value}\"\n"),
                1,
            )
        };
        std::fs::write(&path, with_ca("certs/ca.pem")).unwrap();
        assert_eq!(
            Config::load(&path).unwrap().catalog["tls_ca_file"],
            root.join("certs/ca.pem").to_string_lossy()
        );
        std::fs::write(&path, with_ca("/etc/ssl/ca.pem")).unwrap();
        assert_eq!(
            Config::load(&path).unwrap().catalog["tls_ca_file"],
            "/etc/ssl/ca.pem"
        );
    }

    fn valid_config() -> Config {
        toml::from_str(include_str!("../../../examples/flow.toml")).unwrap()
    }

    #[test]
    fn catalog_secret_references_resolve_without_changing_literal_properties() {
        let mut config = valid_config();
        let original = config.catalog.clone();
        assert_eq!(
            config
                .resolve_catalog_properties(|_| unreachable!())
                .unwrap(),
            original
        );
        for key in ["token", "credential"] {
            config
                .catalog
                .insert(format!("{key}_env"), format!("FLOW_{key}"));
        }
        config.validate().unwrap();
        let resolved = config
            .resolve_catalog_properties(|name| match name {
                "FLOW_token" => Some("test-token".into()),
                "FLOW_credential" => Some("client:test-secret".into()),
                _ => unreachable!(),
            })
            .unwrap();
        let mut expected = original;
        expected.insert("token".into(), "test-token".into());
        expected.insert("credential".into(), "client:test-secret".into());
        assert_eq!(resolved, expected);
        assert!(config.catalog.contains_key("token_env"));
        for key in ["token", "credential"] {
            config.catalog.insert(key.into(), "test-secret".into());
            assert!(
                config
                    .validate()
                    .unwrap_err()
                    .to_string()
                    .contains("configure only one")
            );
            config.catalog.remove(key);
        }
    }

    #[test]
    fn example_compaction_section_documents_the_defaults() {
        let example = include_str!("../../../examples/flow.toml");
        let (_, section) = example.split_once("\n# [compaction]\n").unwrap();
        let keys = section
            .lines()
            .take_while(|line| line.starts_with('#'))
            .filter_map(|line| line.strip_prefix("# "))
            .filter(|line| {
                line.split_once(" = ").is_some_and(|(key, _)| {
                    key.chars()
                        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 34);
        let documented: Config =
            toml::from_str(&format!("{example}\n[compaction]\n{}\n", keys.join("\n"))).unwrap();
        assert_eq!(
            format!("{:?}", documented.compaction),
            format!("{:?}", flow_compactor::Policy::default())
        );
    }

    #[test]
    fn delete_debt_defaults_to_consolidate_and_accepts_current() {
        let example = include_str!("../../../examples/flow.toml");
        assert_eq!(
            valid_config().compaction.delete_debt,
            flow_compactor::DeleteDebt::Consolidate
        );
        for (value, expected) in [
            ("current", flow_compactor::DeleteDebt::Current),
            ("consolidate", flow_compactor::DeleteDebt::Consolidate),
        ] {
            let config: Config = toml::from_str(&format!(
                "{example}\n[compaction]\ndelete_debt = \"{value}\"\n"
            ))
            .unwrap();
            assert_eq!(config.compaction.delete_debt, expected);
        }
        assert!(
            toml::from_str::<Config>(&format!(
                "{example}\n[compaction]\ndelete_debt = \"rewrite\"\n"
            ))
            .is_err()
        );
    }

    #[test]
    fn delete_layout_defaults_to_per_target_and_accepts_shared() {
        let example = include_str!("../../../examples/flow.toml");
        assert_eq!(
            valid_config().compaction.delete_layout,
            flow_compactor::DeleteLayout::PerTarget
        );
        for (value, expected) in [
            ("shared", flow_compactor::DeleteLayout::Shared),
            ("per_target", flow_compactor::DeleteLayout::PerTarget),
            ("per-target", flow_compactor::DeleteLayout::PerTarget),
        ] {
            let config: Config = toml::from_str(&format!(
                "{example}\n[compaction]\ndelete_layout = \"{value}\"\n"
            ))
            .unwrap();
            assert_eq!(config.compaction.delete_layout, expected);
        }
        assert!(
            toml::from_str::<Config>(&format!(
                "{example}\n[compaction]\ndelete_layout = \"vector\"\n"
            ))
            .is_err()
        );
    }

    #[test]
    fn example_statistics_section_documents_the_defaults() {
        let example = include_str!("../../../examples/flow.toml");
        let (_, section) = example.split_once("\n# [statistics]\n").unwrap();
        let keys = section
            .lines()
            .take_while(|line| line.starts_with('#'))
            .filter_map(|line| line.strip_prefix("# "))
            .filter(|line| {
                line.split_once(" = ")
                    .is_some_and(|(key, _)| key.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
            })
            .collect::<Vec<_>>();
        assert_eq!(keys.len(), 8);
        let documented: Config =
            toml::from_str(&format!("{example}\n[statistics]\n{}\n", keys.join("\n"))).unwrap();
        assert_eq!(documented.statistics, Statistics::default());
        assert!(
            !valid_config().statistics.enabled,
            "statistics are off by default"
        );
    }

    #[test]
    fn statistics_select_columns_by_name_or_kind() {
        use flow_model::{Column, ColumnType};
        let mut config = valid_config();
        let column = |field_id, name: &str, data_type| Column {
            field_id,
            name: name.into(),
            data_type,
            nullable: true,
        };
        config.tables[0].columns = vec![
            column(1, "id", ColumnType::Int64),
            column(2, "score", ColumnType::Float64),
            column(3, "payload", ColumnType::String),
            column(4, "status", ColumnType::String),
            column(5, "created_at", ColumnType::TimestampTzMicros),
            column(6, "flag", ColumnType::Bool),
        ];
        let fields = |config: &Config| {
            let table = &config.tables[0];
            table
                .sketch_selection(&config.statistics, &table.schema(0))
                .map(|selection| {
                    selection.map(|selection| {
                        selection
                            .columns
                            .iter()
                            .map(|column| column.field_id)
                            .collect::<Vec<_>>()
                    })
                })
        };
        assert_eq!(fields(&config).unwrap(), None, "off by default");
        config.statistics.enabled = true;
        assert_eq!(fields(&config).unwrap(), Some(vec![1, 3, 4, 5]));
        config.statistics.max_columns = 2;
        assert_eq!(fields(&config).unwrap(), Some(vec![1, 3]));
        config.tables[0].statistics = Some(StatisticsColumns::Columns(vec![
            "score".into(),
            "id".into(),
        ]));
        assert_eq!(fields(&config).unwrap(), Some(vec![1, 2]), "schema order");
        config.tables[0].statistics = Some(StatisticsColumns::None);
        assert_eq!(fields(&config).unwrap(), None);
        config.statistics.columns = StatisticsColumns::None;
        config.tables[0].statistics = None;
        assert_eq!(fields(&config).unwrap(), None);
        for (columns, error) in [
            (vec!["missing"], "not a configured column"),
            (vec!["id", "id"], "twice"),
            (vec![], "no columns"),
        ] {
            config.tables[0].statistics = Some(StatisticsColumns::Columns(
                columns.into_iter().map(String::from).collect(),
            ));
            let message = format!("{:#}", fields(&config).unwrap_err());
            assert!(message.contains(error), "{message}");
        }
        #[derive(Deserialize)]
        struct Wrapper {
            statistics: StatisticsColumns,
        }
        for (text, parsed) in [
            ("\"auto\"", Some(StatisticsColumns::Auto)),
            ("\"none\"", Some(StatisticsColumns::None)),
            (
                "[\"id\"]",
                Some(StatisticsColumns::Columns(vec!["id".into()])),
            ),
            ("\"all\"", None),
        ] {
            assert_eq!(
                toml::from_str::<Wrapper>(&format!("statistics = {text}"))
                    .ok()
                    .map(|wrapper| wrapper.statistics),
                parsed
            );
        }
    }

    #[test]
    fn partition_is_declared_only_and_parses_iceberg_transforms() {
        use iceberg::spec::Transform;
        #[derive(Deserialize)]
        struct Wrapper {
            partition: PartitionSetting,
        }
        let with = |partition: Option<&str>| {
            let mut config = valid_config();
            config.tables[0].partition = partition.map(|partition| {
                toml::from_str::<Wrapper>(&format!("partition = {partition}"))
                    .unwrap()
                    .partition
            });
            config
        };
        let spec = |config: &Config| {
            let table = &config.tables[0];
            table.partition_spec(&flow_materializer::iceberg_schema(&table.schema(0)).unwrap())
        };
        // Absent: Flow neither chooses nor changes a spec.
        assert!(spec(&with(None)).unwrap().is_none());
        assert!(
            spec(&with(Some("\"none\"")))
                .unwrap()
                .unwrap()
                .is_unpartitioned()
        );
        let declared = with(Some(
            r#"["status", "bucket(16, id)", "truncate(4, status)"]"#,
        ));
        assert!(declared.validate().is_ok());
        let fields: Vec<_> = spec(&declared)
            .unwrap()
            .unwrap()
            .fields()
            .iter()
            .map(|field| (field.source_id, field.name.clone(), field.transform))
            .collect();
        assert_eq!(
            fields,
            [
                (2, "status".into(), Transform::Identity),
                (1, "id_bucket".into(), Transform::Bucket(16)),
                (2, "status_trunc".into(), Transform::Truncate(4)),
            ]
        );
        assert_eq!(
            partition_field(" Day( created_at ) ").unwrap(),
            (Transform::Day, "created_at")
        );
        assert_eq!(
            partition_field("months(created_at)").unwrap(),
            (Transform::Month, "created_at")
        );
        for invalid in [
            r#"["missing"]"#,
            r#"["bucket(0, id)"]"#,
            r#"["bucket(id)"]"#,
            r#"["day(status)"]"#,
            r#"["zorder(id)"]"#,
            r#"["status", "status"]"#,
            "[]",
        ] {
            assert!(
                with(Some(invalid)).validate().is_err(),
                "{invalid} was accepted"
            );
        }
        assert!(toml::from_str::<Wrapper>("partition = \"auto\"").is_err());
        let mut background = valid_config();
        assert_eq!(
            background.tables[0].partition_convert,
            PartitionConvert::Lazy
        );
        background.tables[0].partition_convert = toml::from_str::<
            std::collections::HashMap<String, PartitionConvert>,
        >("c = \"background\"")
        .unwrap()["c"];
        assert_eq!(
            background.tables[0].partition_convert,
            PartitionConvert::Background
        );
    }

    #[test]
    fn sort_defaults_to_the_primary_key_and_parses_explicit_orders() {
        use flow_materializer::SortKey;
        let key = |column, descending, nulls_first| SortKey {
            column,
            descending,
            nulls_first,
        };
        let with = |sort: Option<&str>| {
            let mut config = valid_config();
            config.tables[0].sort = sort.map(|sort| {
                toml::from_str::<Wrapper>(&format!("sort = {sort}"))
                    .unwrap()
                    .sort
            });
            config
        };
        #[derive(Deserialize)]
        struct Wrapper {
            sort: SortSetting,
        }
        assert!(with(None).validate().is_ok());
        assert!(with(Some(r#"["status", "id DESC"]"#)).validate().is_ok());
        assert_eq!(
            with(None).tables[0].sort_keys().unwrap(),
            [key(0, false, true)]
        );
        assert_eq!(
            with(Some("\"primary_key\"")).tables[0].sort_keys().unwrap(),
            [key(0, false, true)]
        );
        assert!(
            with(Some("\"none\"")).tables[0]
                .sort_keys()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            with(Some(r#"["status", "id DESC"]"#)).tables[0]
                .sort_keys()
                .unwrap(),
            [key(1, false, true), key(0, true, false)]
        );
        assert_eq!(
            with(Some(r#"["status desc nulls first", "id asc nulls last"]"#)).tables[0]
                .sort_keys()
                .unwrap(),
            [key(1, true, true), key(0, false, false)]
        );
        for invalid in [
            r#"["missing"]"#,
            r#"["id sideways"]"#,
            r#"["id", "id desc"]"#,
            r#"["id nulls"]"#,
            "[]",
        ] {
            assert!(with(Some(invalid)).validate().is_err(), "{invalid}");
        }
        assert!(toml::from_str::<Wrapper>("sort = \"by_magic\"").is_err());
        let mut keyless = with(Some("\"primary_key\""));
        keyless.tables[0].primary_key.clear();
        keyless.tables[0].append_only = true;
        assert!(keyless.validate().is_err());
        keyless.tables[0].sort = None;
        assert!(
            keyless.tables[0].sort_keys().unwrap().is_empty(),
            "keyless tables are unsorted"
        );
    }

    #[test]
    fn rejects_limits_that_downstream_fixed_configuration_cannot_use() {
        type Invalidate = fn(&mut Limits);
        type Case = (&'static str, Invalidate, &'static str);

        valid_config().validate().unwrap();
        let cases: [Case; 8] = [
            (
                "no spool transactions",
                |limits| {
                    limits.spool_transactions = 0;
                },
                "invalid spool limits",
            ),
            (
                "no spool subtransactions",
                |limits| {
                    limits.spool_subtransactions = 0;
                },
                "invalid spool limits",
            ),
            (
                "journal quota below one segment",
                |limits| {
                    limits.journal_bytes = JournalConfig::default().segment_bytes - 1;
                },
                "invalid journal limits",
            ),
            (
                "spool quota below one segment",
                |limits| {
                    limits.spool_bytes = SpoolConfig::default().segment_bytes - 1;
                },
                "invalid spool limits",
            ),
            (
                "chunk larger than a journal frame",
                |limits| {
                    limits.chunk_bytes =
                        u32::try_from(JournalConfig::default().segment_bytes).unwrap();
                    limits.source_message_bytes = limits.chunk_bytes as usize;
                    limits.batch_bytes = limits.chunk_bytes as usize;
                    limits.parquet_row_group_bytes = limits.batch_bytes;
                },
                "invalid journal limits",
            ),
            (
                "zero garbage interval",
                |limits| {
                    limits.garbage_interval_secs = 0;
                },
                "garbage interval and orphan grace must be positive",
            ),
            (
                "zero orphan grace",
                |limits| {
                    limits.orphan_grace_secs = 0;
                },
                "garbage interval and orphan grace must be positive",
            ),
            (
                "zero metadata JSON grace",
                |limits| {
                    limits.metadata_json_grace_secs = Some(0);
                },
                "garbage interval and orphan grace must be positive",
            ),
        ];

        for (case, invalidate, expected) in cases {
            let mut config = valid_config();
            invalidate(&mut config.limits);
            let error = config.validate().expect_err(case);
            assert!(
                format!("{error:#}").contains(expected),
                "{case} returned {error:#}"
            );
        }
    }

    #[test]
    fn memory_backed_state_requires_remote_durability() {
        let mut config = valid_config();
        assert_eq!(config.storage.medium, Medium::Disk);
        config.storage.medium = Medium::Memory;
        let error = config.validate().unwrap_err();
        assert!(format!("{error:#}").contains("requires [durability]"));
        config.durability = Some(flow_durability::Config::new("memory://state"));
        config.validate().unwrap();
        let parsed: Storage = toml::from_str("medium = \"memory\"").unwrap();
        assert_eq!(parsed.medium, Medium::Memory);
    }
}

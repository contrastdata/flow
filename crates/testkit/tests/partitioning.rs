//! Partitioned tables: ingest epochs write the unpartitioned spec, rewrites
//! write the table's default spec split by partition, and every delete is in
//! its target's spec and partition. Rows, the index and every file's
//! partition stay exact through CDC, strict and background rewrites, deletes
//! that move rows between partitions, and spec changes.

use flow_compactor::Policy;
use flow_coordinator::{DeleteRewritePolicy, TableMaintenance, TablePublisher};
use flow_iceberg_ext::{SnapshotView, exact_delete_target, ingest_spec_id};
use flow_materializer::{WriterConfig, iceberg_schema};
use flow_model::{FileId, PgLsn, Row, SourceId, TableSchema, Value};
use flow_state_store::{Change, StateStore, StateStoreOptions};
use flow_testkit::{LostResponseCatalog, catalog, scan, schema};
use iceberg::{
    Catalog, NamespaceIdent, TableCommit, TableCreation, TableRequirement, TableUpdate,
    spec::{
        DataContentType, Datum, FormatVersion, Literal, NullOrder, PartitionSpec, Struct, Transform,
    },
    table::Table,
    transaction::{ApplyTransactionAction, Transaction},
    transform::create_transform_function,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tempfile::TempDir;

/// Buffered epochs are written in the table's sort order and stamped.
const BUFFERED: usize = 8 << 20;

fn row(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}

fn id_of(row: &Row) -> i64 {
    match row[0] {
        Value::Int64(id) => id,
        _ => unreachable!(),
    }
}

fn policy() -> Policy {
    Policy {
        l0_soft_files: 2,
        l0_hard_files: 4,
        oldest_l0_soft_ms: 1,
        oldest_l0_hard_ms: 2,
        stable_small_soft_files: 2,
        stable_small_hard_files: 8,
        deleted_rows_percent: 100,
        min_file_age_ms: 0,
        lazy_index_remap: true,
        sort_merge: true,
        ..Default::default()
    }
}

/// `bucket(4, id)` and the first character of `value`: an update that
/// changes the value's first character moves its row to another partition.
fn bucket_and_prefix() -> Vec<(&'static str, Transform)> {
    vec![
        ("id", Transform::Bucket(4)),
        ("value", Transform::Truncate(1)),
    ]
}

/// One live data file and the rows it holds.
#[derive(Debug)]
struct LiveFile {
    path: String,
    spec_id: i32,
    partition: Struct,
    order: Option<i32>,
    ids: Vec<i64>,
}

struct Fixture {
    temp: TempDir,
    catalog: Arc<LostResponseCatalog>,
    index: StateStore,
    schema: TableSchema,
    head: Table,
    /// Every live row by id, as the source has it.
    model: BTreeMap<i64, Row>,
    lsn: u64,
    scratches: usize,
}

impl Fixture {
    async fn new(version: FormatVersion, spec: Option<&[(&str, Transform)]>) -> Self {
        let temp = TempDir::new().unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let iceberg = iceberg_schema(&schema).unwrap();
        let mut creation = TableCreation::builder()
            .name("partitioned".into())
            .format_version(version)
            .schema(iceberg.clone())
            .build();
        // A table created partitioned has no unpartitioned spec at all.
        if let Some(fields) = spec {
            creation.partition_spec = Some(bound(&iceberg, fields).into_unbound());
        }
        let head = catalog
            .create_table(&NamespaceIdent::new("test".into()), creation)
            .await
            .unwrap();
        let index = StateStore::open(
            temp.path().join("index"),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap();
        let mut fixture = Self {
            temp,
            catalog,
            index,
            schema,
            head,
            model: BTreeMap::new(),
            lsn: 10,
            scratches: 0,
        };
        fixture.order_by_id().await;
        fixture
    }

    async fn order_by_id(&mut self) {
        let transaction = Transaction::new(&self.head);
        self.head = transaction
            .replace_sort_order()
            .asc("id", NullOrder::First)
            .apply(transaction)
            .unwrap()
            .commit(self.catalog.as_ref())
            .await
            .unwrap();
    }

    /// Make `fields` the default spec, as `ALTER TABLE ... ADD PARTITION
    /// FIELD` does: earlier files keep their spec.
    async fn partition_by(&mut self, fields: &[(&str, Transform)]) {
        let metadata = self.head.metadata();
        let spec = bound(metadata.current_schema(), fields);
        self.head = self
            .catalog
            .update_table(
                TableCommit::builder()
                    .ident(self.head.identifier().clone())
                    .requirements(vec![TableRequirement::DefaultSpecIdMatch {
                        default_spec_id: metadata.default_partition_spec_id(),
                    }])
                    .updates(vec![
                        TableUpdate::AddSpec {
                            spec: spec.into_unbound(),
                        },
                        TableUpdate::SetDefaultSpec { spec_id: -1 },
                    ])
                    .build(),
            )
            .await
            .unwrap();
    }

    fn scratch(&mut self) -> StateStore {
        self.scratches += 1;
        StateStore::open(
            self.temp.path().join(format!("scratch-{}", self.scratches)),
            StateStoreOptions {
                apply_batch_rows: 3,
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn maintenance(&self, policy: Policy) -> TableMaintenance {
        TableMaintenance::new(
            self.index.clone(),
            self.catalog.clone(),
            policy,
            WriterConfig::default(),
        )
        .unwrap()
    }

    async fn refresh(&mut self) {
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
    }

    /// Publish one epoch of CDC and apply it to the model.
    async fn cdc(&mut self, changes: Vec<(i64, Change)>, memory_bytes: usize) {
        self.lsn += 10;
        for (id, change) in &changes {
            match change {
                Change::Insert(row) | Change::Update(row) => {
                    self.model.insert(*id, row.clone());
                }
                Change::Delete => {
                    self.model.remove(id);
                }
            }
        }
        let (epoch, collapsed) = flow_testkit::collapse_fixture_with(
            &self.index,
            &self.head,
            &self.schema,
            SourceId("fixture".into()),
            PgLsn(self.lsn),
            changes
                .into_iter()
                .map(|(id, change)| (self.schema.encode_key(&row(id, "")).unwrap(), change)),
            memory_bytes,
        );
        TablePublisher::new(
            self.index.clone(),
            self.catalog.clone(),
            WriterConfig::default(),
            2,
            1 << 20,
        )
        .unwrap()
        .publish(&self.head, &self.schema, collapsed)
        .await
        .unwrap();
        self.index.forget_applied(&epoch.id).unwrap();
        self.refresh().await;
    }

    async fn insert(&mut self, ids: impl IntoIterator<Item = i64>, value: &str) {
        let changes = ids
            .into_iter()
            .map(|id| (id, Change::Insert(row(id, &format!("{value}-{id}")))))
            .collect();
        self.cdc(changes, BUFFERED).await;
    }

    async fn compact(&mut self, maintenance: &TableMaintenance) -> bool {
        let head = self.head.clone();
        let scratch = self.scratch();
        let published = maintenance
            .compact(&head, &self.schema, scratch)
            .await
            .unwrap()
            .is_some();
        self.refresh().await;
        published
    }

    /// Checks Iceberg's rows against the model with two independent readers,
    /// the index against every live row, and every file's spec and
    /// partition; returns the live data files.
    async fn audit(&mut self) -> Vec<LiveFile> {
        let expected: Vec<Row> = self.model.values().cloned().collect();
        let mut scanned = scan(&self.head, &self.schema).await.unwrap();
        scanned.sort_by_key(id_of);
        assert_eq!(
            scanned, expected,
            "iceberg-rust scan differs from the source"
        );

        let head = self.head.clone();
        let metadata = head.metadata();
        let ingest = ingest_spec_id(metadata);
        let view = SnapshotView::current(&self.head).await.unwrap();
        let files: BTreeSet<_> = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| FileId(entry.file_path().into()))
            .collect();
        let scratch = self.scratch();
        let mut live = Vec::new();
        flow_compactor::scan_live_files(
            &self.head,
            &self.schema,
            &view,
            &files,
            &scratch,
            "audit",
            3,
            &flow_compactor::ReadLimits::default(),
            async |batch| {
                for (row, position) in batch.rows.into_iter().zip(batch.positions) {
                    live.push((row, batch.file_id.clone(), position));
                }
                Ok(())
            },
        )
        .await
        .unwrap();
        let mut rows: Vec<Row> = live.iter().map(|(row, _, _)| row.clone()).collect();
        rows.sort_by_key(id_of);
        assert_eq!(rows, expected, "Flow's live scan differs from the source");
        let keys: Vec<_> = live
            .iter()
            .map(|(row, _, _)| self.schema.encode_key(row).unwrap())
            .collect();
        let located = self
            .index
            .lookup_many(&self.schema.table_id, &keys)
            .unwrap();
        for ((row, file, position), location) in live.iter().zip(located) {
            let location = location.unwrap_or_else(|| panic!("{} is not indexed", id_of(row)));
            assert_eq!(
                (&location.data_file_id, location.row_position),
                (file, *position),
                "{} is indexed at a different row",
                id_of(row)
            );
            assert_eq!(
                location.spec_id,
                view.live_files[&file.0].data_file.partition_spec_id(),
                "{} is indexed with another file's spec",
                id_of(row)
            );
        }
        let ordered: Vec<_> = files.iter().cloned().collect();
        let counts = self
            .index
            .file_live_row_counts(
                &self.schema.table_id,
                self.head.metadata().current_snapshot_id(),
                &ordered,
            )
            .unwrap();
        for (file, count) in ordered.iter().zip(counts) {
            assert_eq!(
                count,
                live.iter().filter(|(_, live, _)| live == file).count() as u64,
                "live count of {}",
                file.0
            );
        }

        // A delete applies only within its target's spec and partition.
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::PositionDeletes)
        {
            if let Some(target) = exact_delete_target(&entry.data_file)
                && let Some(data) = view.live_files.get(&target)
            {
                assert_eq!(
                    (
                        entry.data_file.partition_spec_id(),
                        entry.data_file.partition()
                    ),
                    (
                        data.data_file.partition_spec_id(),
                        data.data_file.partition()
                    ),
                    "delete {} is outside its target's partition",
                    entry.file_path()
                );
            }
        }

        let mut result = Vec::new();
        for file in files {
            let entry = &view.live_files[&file.0];
            let spec_id = entry.data_file.partition_spec_id();
            let spec = metadata.partition_spec_by_id(spec_id).unwrap();
            let bytes = self
                .head
                .file_io()
                .new_input(&file.0)
                .unwrap()
                .read()
                .await
                .unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
            let mut ids = Vec::new();
            for batch in builder.build().unwrap() {
                for row in
                    flow_materializer::rows_from_batch(&self.schema, &batch.unwrap()).unwrap()
                {
                    // Every row of a file is in that file's partition.
                    assert_eq!(
                        &partition_of(spec, &row),
                        entry.data_file.partition(),
                        "row {} of {} is outside its file's partition",
                        id_of(&row),
                        file.0
                    );
                    ids.push(id_of(&row));
                }
            }
            let order = entry.data_file.sort_order_id();
            if order.is_some() {
                assert!(ids.is_sorted(), "{} is stamped but out of order", file.0);
            }
            if file.0.contains("flow-l0-") {
                assert_eq!(Some(spec_id), ingest, "ingest files use the ingest spec");
            }
            result.push(LiveFile {
                path: file.0,
                spec_id,
                partition: entry.data_file.partition().clone(),
                order,
                ids,
            });
        }
        result
    }
}

fn bound(schema: &iceberg::spec::Schema, fields: &[(&str, Transform)]) -> PartitionSpec {
    let mut builder = PartitionSpec::builder(schema.clone());
    for (column, transform) in fields {
        builder = builder
            .add_partition_field(*column, format!("{column}_{}", name(transform)), *transform)
            .unwrap();
    }
    builder.build().unwrap()
}

fn name(transform: &Transform) -> &'static str {
    match transform {
        Transform::Bucket(_) => "bucket",
        Transform::Truncate(_) => "trunc",
        Transform::Day => "day",
        _ => "identity",
    }
}

/// The partition tuple `spec` assigns to a row.
fn partition_of(spec: &PartitionSpec, row: &Row) -> Struct {
    Struct::from_iter(spec.fields().iter().map(|field| {
        let value = match (&row[field.source_id as usize - 1], field.source_id) {
            (Value::Int64(id), _) => Datum::long(*id),
            (Value::String(text), _) => Datum::string(text),
            (Value::Null, _) => return None,
            other => panic!("unexpected partition source {other:?}"),
        };
        create_transform_function(&field.transform)
            .unwrap()
            .transform_literal(&value)
            .unwrap()
            .map(|datum| Literal::Primitive(datum.literal().clone()))
    }))
}

/// Files written by rewrites, which Flow names after the operation.
fn rewritten(files: &[LiveFile]) -> Vec<&LiveFile> {
    files
        .iter()
        .filter(|file| file.path.contains("-rewrite-"))
        .collect()
}

async fn ingest_then_partition(version: FormatVersion) {
    let mut f = Fixture::new(version, None).await;
    f.partition_by(&bucket_and_prefix()).await;
    let maintenance = f.maintenance(policy());
    f.insert(0..30, "a").await;
    f.insert(30..60, "b").await;
    let files = f.audit().await;
    assert_eq!(files.len(), 2);
    assert!(
        files
            .iter()
            .all(|file| file.spec_id == 0 && file.partition.fields().is_empty()),
        "ingest epochs write the unpartitioned spec: {files:?}"
    );

    assert!(f.compact(&maintenance).await);
    let files = f.audit().await;
    let outputs = rewritten(&files);
    assert_eq!(outputs.len(), files.len(), "{files:?}");
    // Four buckets of each of the two value prefixes, one file each.
    let partitions: BTreeSet<_> = outputs
        .iter()
        .map(|file| format!("{:?}", file.partition))
        .collect();
    assert_eq!(partitions.len(), 8, "{files:?}");
    assert_eq!(outputs.len(), 8, "{files:?}");
    let mut ids: Vec<i64> = outputs.iter().flat_map(|file| file.ids.clone()).collect();
    ids.sort();
    assert_eq!(ids, (0..60).collect::<Vec<_>>(), "rows move once each");
    assert!(outputs.iter().all(|file| file.order == Some(1)));

    // Deletes on partitioned files go to their partitions; updates that
    // change the prefix move rows, through ingest, to other partitions.
    f.cdc(
        vec![
            (0, Change::Delete),
            (7, Change::Update(row(7, "z-moved"))),
            (31, Change::Update(row(31, "b-same-prefix"))),
            (44, Change::Delete),
            (59, Change::Update(row(59, "a-moved"))),
            (60, Change::Insert(row(60, "c-new"))),
        ],
        BUFFERED,
    )
    .await;
    f.audit().await;
    f.cdc(
        vec![
            (7, Change::Update(row(7, "a-back"))),
            (60, Change::Delete),
            (3, Change::Update(row(3, "a-again"))),
        ],
        0,
    )
    .await;
    f.audit().await;
    while f.compact(&maintenance).await {
        f.audit().await;
    }
    let files = f.audit().await;
    assert!(
        files
            .iter()
            .all(|file| file.spec_id == f.head.metadata().default_partition_spec_id()),
        "compaction leaves no ingest file behind: {files:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_writes_the_unpartitioned_spec_and_compaction_partitions_v2() {
    ingest_then_partition(FormatVersion::V2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_writes_the_unpartitioned_spec_and_compaction_partitions_v3() {
    ingest_then_partition(FormatVersion::V3).await;
}

/// A table created partitioned has no unpartitioned spec. The first epoch
/// adds one without changing the default.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_table_created_partitioned_gains_an_ingest_spec() {
    let mut f = Fixture::new(FormatVersion::V2, Some(&bucket_and_prefix())).await;
    assert_eq!(ingest_spec_id(f.head.metadata()), None);
    let default = f.head.metadata().default_partition_spec_id();
    f.insert(0..20, "a").await;
    let ingest = ingest_spec_id(f.head.metadata()).expect("ingest spec added");
    assert_ne!(ingest, default);
    assert_eq!(f.head.metadata().default_partition_spec_id(), default);
    let files = f.audit().await;
    assert!(files.iter().all(|file| file.spec_id == ingest));
    let maintenance = f.maintenance(policy());
    f.insert(20..40, "b").await;
    assert!(f.compact(&maintenance).await);
    let files = f.audit().await;
    assert!(
        files.iter().all(|file| file.spec_id == default),
        "{files:?}"
    );
}

/// Files written before a spec change keep their spec. Deletes on them stay
/// in it; compaction rewrites ingest files into the new spec, and old files
/// it selects as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_spec_change_keeps_old_files_and_rewrites_into_the_new_spec() {
    let mut f = Fixture::new(FormatVersion::V2, None).await;
    let maintenance = f.maintenance(policy());
    f.insert(0..40, "a").await;
    f.insert(40..80, "b").await;
    assert!(f.compact(&maintenance).await);
    let before = f.audit().await;
    assert!(before.iter().all(|file| file.spec_id == 0));

    f.partition_by(&[("id", Transform::Bucket(2))]).await;
    let target = f.head.metadata().default_partition_spec_id();
    f.cdc(
        vec![
            (1, Change::Delete),
            (2, Change::Update(row(2, "updated"))),
            (100, Change::Insert(row(100, "new"))),
        ],
        BUFFERED,
    )
    .await;
    f.insert(200..220, "c").await;
    let files = f.audit().await;
    assert!(files.iter().all(|file| file.spec_id == 0));
    while f.compact(&maintenance).await {
        f.audit().await;
    }
    let files = f.audit().await;
    assert!(
        files.iter().any(|file| file.spec_id == target),
        "ingest files were rewritten into the new spec: {files:?}"
    );
    // Deletes on old-spec files and new ones alike keep rows exact.
    let changes = f
        .model
        .keys()
        .copied()
        .step_by(3)
        .map(|id| (id, Change::Delete))
        .collect();
    f.cdc(changes, BUFFERED).await;
    f.audit().await;
}

/// A background build writes partitioned output while CDC continues; late
/// changes to its inputs are translated into deletes in the outputs'
/// partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_builds_catch_up_late_changes_into_partitioned_outputs() {
    for version in [FormatVersion::V2, FormatVersion::V3] {
        let mut f = Fixture::new(version, None).await;
        f.partition_by(&bucket_and_prefix()).await;
        let maintenance = f.maintenance(policy());
        f.insert((0..60).step_by(2), "a").await;
        f.insert((1..60).step_by(2), "b").await;
        let head = f.head.clone();
        let scratch = f.scratch();
        let running = maintenance
            .start_compaction(&head, &f.schema, scratch)
            .await
            .unwrap()
            .unwrap();
        f.cdc(
            vec![
                (0, Change::Delete),
                (1, Change::Update(row(1, "late"))),
                (31, Change::Delete),
                (58, Change::Update(row(58, "zz-late"))),
                (100, Change::Insert(row(100, "late"))),
            ],
            BUFFERED,
        )
        .await;
        if version == FormatVersion::V2 {
            let scratch = f.scratch();
            maintenance
                .compact_deletes(
                    &f.head,
                    &f.schema,
                    scratch,
                    &DeleteRewritePolicy {
                        min_input_files: 2,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            f.refresh().await;
        }
        let ready = running.wait().await.unwrap();
        let head = f.head.clone();
        assert!(
            maintenance
                .finish_compaction(&head, &f.schema, ready)
                .await
                .unwrap()
                .is_some()
        );
        f.refresh().await;
        let files = f.audit().await;
        assert!(rewritten(&files).len() > 1, "{files:?}");
        f.cdc(
            vec![(4, Change::Update(row(4, "after"))), (57, Change::Delete)],
            BUFFERED,
        )
        .await;
        f.audit().await;
    }
}

/// A small deterministic generator, so failures reproduce from the seed.
struct Random(u64);

impl Random {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Random CDC whose updates move rows between partitions, buffered and disk
/// epochs, and strict and background rewrites with late changes: rows,
/// index and every partition stay exact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn random_cdc_and_rewrites_keep_rows_index_and_partitions_exact() {
    let seeds: u64 = std::env::var("FLOW_PARTITION_SEEDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let mut partitioned = 0;
    for seed in 1..=seeds {
        let mut random = Random(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let version = if seed % 2 == 0 {
            FormatVersion::V3
        } else {
            FormatVersion::V2
        };
        let mut f = Fixture::new(version, None).await;
        let maintenance = f.maintenance(Policy {
            l1_target_bytes: if seed % 3 == 0 { 4 << 10 } else { 128 << 20 },
            ..policy()
        });
        let epoch = |f: &mut Fixture, random: &mut Random| {
            let mut changes = BTreeMap::new();
            for _ in 0..random.below(24) + 1 {
                let id = random.below(200) as i64 - 50;
                let prefix = ["a", "b", "u"][random.below(3) as usize];
                let value = format!("{prefix}{}", random.below(1000));
                let change = match (f.model.contains_key(&id), random.below(3)) {
                    (false, _) => Change::Insert(row(id, &value)),
                    (true, 0) => Change::Delete,
                    (true, _) => Change::Update(row(id, &value)),
                };
                changes.entry(id).or_insert(change);
            }
            let memory = if random.below(4) == 0 { 0 } else { BUFFERED };
            (changes.into_iter().collect::<Vec<_>>(), memory)
        };
        for step in 0..12 {
            if step == 3 {
                // Partition a table that already has data.
                f.partition_by(&bucket_and_prefix()).await;
            }
            let (changes, memory) = epoch(&mut f, &mut random);
            f.cdc(changes, memory).await;
            if step % 3 != 2 {
                continue;
            }
            if random.below(2) == 0 {
                f.compact(&maintenance).await;
            } else {
                let head = f.head.clone();
                let scratch = f.scratch();
                let Some(running) = maintenance
                    .start_compaction(&head, &f.schema, scratch)
                    .await
                    .unwrap()
                else {
                    continue;
                };
                for _ in 0..random.below(3) {
                    let (changes, memory) = epoch(&mut f, &mut random);
                    f.cdc(changes, memory).await;
                }
                let ready = running.wait().await.unwrap();
                let head = f.head.clone();
                maintenance
                    .finish_compaction(&head, &f.schema, ready)
                    .await
                    .unwrap();
                f.refresh().await;
            }
            let files = f.audit().await;
            partitioned += files
                .iter()
                .filter(|file| !file.partition.fields().is_empty())
                .count();
        }
    }
    assert!(partitioned > 0, "no file was partitioned");
}

/// Old-spec files stay put unless rewritten for another reason; a table
/// converting in the background rewrites them into its spec within budget.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_conversion_moves_old_spec_files_within_budget() {
    let mut f = Fixture::new(FormatVersion::V2, None).await;
    let lazy = f.maintenance(policy());
    f.insert(0..40, "a").await;
    f.insert(40..80, "b").await;
    assert!(f.compact(&lazy).await);
    let before = f.audit().await;
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].spec_id, 0);

    f.partition_by(&[("id", Transform::Bucket(4))]).await;
    let target = f.head.metadata().default_partition_spec_id();
    assert!(!f.compact(&lazy).await, "lazy conversion leaves old files");

    let none = f
        .maintenance(Policy {
            partition_convert_bytes_per_hour: 0,
            ..policy()
        })
        .with_partition_conversion([f.schema.table_id]);
    assert!(!f.compact(&none).await, "a zero budget converts nothing");

    let converting = f
        .maintenance(policy())
        .with_partition_conversion([f.schema.table_id]);
    assert!(f.compact(&converting).await);
    let after = f.audit().await;
    assert_eq!(after.len(), 4, "{after:?}");
    assert!(after.iter().all(|file| file.spec_id == target));
    f.insert(100..110, "c").await;
    while f.compact(&converting).await {
        f.audit().await;
    }
    let files = f.audit().await;
    assert!(files.iter().all(|file| file.spec_id == target));
}

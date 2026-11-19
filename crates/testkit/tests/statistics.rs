//! Table statistics: compaction outputs carry Theta sketches of their
//! columns, a statistics visit unions them into one Iceberg statistics file
//! of the head snapshot, and the file survives history expiration and
//! garbage collection for as long as it is current. Ingest (L0) files are
//! never sketched.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use flow_compactor::Policy;
use flow_coordinator::{
    GarbagePolicy, GarbageProtection, HistoryPolicy, StatisticsPolicy, TableMaintenance,
    TablePublisher,
};
use flow_iceberg_ext::SnapshotView;
use flow_materializer::statistics::read_sketches;
use flow_materializer::{SketchSelection, WriterConfig, iceberg_schema, sketch_path};
use flow_model::{PgLsn, Row, SourceId, TableSchema, Value};
use flow_state_store::{Change, StateStore, StateStoreOptions};
use flow_testkit::{LostResponseCatalog, catalog, schema};
use iceberg::{
    Catalog, NamespaceIdent, TableCommit, TableCreation, TableUpdate,
    spec::{DataContentType, StatisticsFile},
    table::Table,
};
use tempfile::TempDir;

fn row(id: i64, value: &str) -> Row {
    vec![Value::Int64(id), Value::String(value.into())]
}

fn compaction() -> Policy {
    Policy {
        l0_soft_files: 2,
        l0_hard_files: 4,
        oldest_l0_soft_ms: 1,
        oldest_l0_hard_ms: 2,
        stable_small_soft_files: 2,
        stable_small_hard_files: 8,
        deleted_rows_percent: 1,
        min_file_age_ms: 0,
        ..Default::default()
    }
}

fn statistics() -> StatisticsPolicy {
    StatisticsPolicy {
        refresh_interval: Duration::ZERO,
        ..Default::default()
    }
}

struct Fixture {
    temp: TempDir,
    catalog: Arc<LostResponseCatalog>,
    index: StateStore,
    schema: TableSchema,
    head: Table,
    /// Live rows by id.
    model: HashMap<i64, String>,
    lsn: u64,
    scratches: usize,
}

impl Fixture {
    async fn new() -> Self {
        let temp = TempDir::new().unwrap();
        let catalog = Arc::new(LostResponseCatalog::new(
            catalog(&temp.path().join("warehouse")).await,
        ));
        let schema = schema(1);
        let head = catalog
            .create_table(
                &NamespaceIdent::new("test".into()),
                TableCreation::builder()
                    .name("sketched".into())
                    .schema(iceberg_schema(&schema).unwrap())
                    .build(),
            )
            .await
            .unwrap();
        let index =
            StateStore::open(temp.path().join("index"), StateStoreOptions::default()).unwrap();
        Self {
            temp,
            catalog,
            index,
            schema,
            head,
            model: HashMap::new(),
            lsn: 10,
            scratches: 0,
        }
    }

    fn selection(&self) -> Arc<SketchSelection> {
        Arc::new(SketchSelection::of_fields(&self.schema, &[1, 2]))
    }

    /// Maintenance that sketches `fields` under `policy`.
    fn sketching(&self, fields: &[i32], policy: StatisticsPolicy) -> TableMaintenance {
        TableMaintenance::new(
            self.index.clone(),
            self.catalog.clone(),
            compaction(),
            WriterConfig::default(),
        )
        .unwrap()
        .with_statistics(
            policy,
            HashMap::from([(
                self.schema.table_id,
                Arc::new(SketchSelection::of_fields(&self.schema, fields)),
            )]),
        )
    }

    fn maintenance(&self, sketched: bool) -> TableMaintenance {
        let maintenance = TableMaintenance::new(
            self.index.clone(),
            self.catalog.clone(),
            compaction(),
            WriterConfig::default(),
        )
        .unwrap();
        if sketched {
            maintenance.with_statistics(
                statistics(),
                HashMap::from([(self.schema.table_id, self.selection())]),
            )
        } else {
            maintenance
        }
    }

    async fn refresh(&mut self) {
        self.head = self
            .catalog
            .load_table(self.head.identifier())
            .await
            .unwrap();
    }

    async fn cdc(&mut self, changes: Vec<(i64, Change)>) {
        self.lsn += 10;
        for (id, change) in &changes {
            match change {
                Change::Insert(row) | Change::Update(row) => {
                    let Value::String(value) = &row[1] else {
                        unreachable!()
                    };
                    self.model.insert(*id, value.clone());
                }
                Change::Delete => {
                    self.model.remove(id);
                }
            }
        }
        let (epoch, collapsed) = flow_testkit::collapse_fixture(
            &self.index,
            &self.head,
            &self.schema,
            SourceId("fixture".into()),
            PgLsn(self.lsn),
            changes
                .into_iter()
                .map(|(id, change)| (self.schema.encode_key(&row(id, "")).unwrap(), change)),
        );
        TablePublisher::new(
            self.index.clone(),
            self.catalog.clone(),
            WriterConfig::default(),
            2,
            1 << 20,
        )
        .unwrap()
        .with_sketches(HashMap::from([(self.schema.table_id, self.selection())]))
        .publish(&self.head, &self.schema, collapsed)
        .await
        .unwrap();
        self.index.forget_applied(&epoch.id).unwrap();
        self.refresh().await;
    }

    async fn insert(&mut self, ids: impl IntoIterator<Item = i64>, value: impl Fn(i64) -> String) {
        let changes = ids
            .into_iter()
            .map(|id| (id, Change::Insert(row(id, &value(id)))))
            .collect();
        self.cdc(changes).await;
    }

    /// Compact until nothing is left to do.
    async fn compact(&mut self, maintenance: &TableMaintenance) {
        for _ in 0..8 {
            self.scratches += 1;
            let scratch = StateStore::open(
                self.temp.path().join(format!("scratch-{}", self.scratches)),
                StateStoreOptions::default(),
            )
            .unwrap();
            let head = self.head.clone();
            let published = maintenance
                .compact(&head, &self.schema, scratch)
                .await
                .unwrap()
                .is_some();
            self.refresh().await;
            if !published {
                return;
            }
        }
    }

    async fn live_data_files(&self) -> Vec<String> {
        let view = SnapshotView::current(&self.head).await.unwrap();
        let mut files: Vec<_> = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| entry.file_path().to_owned())
            .collect();
        files.sort();
        files
    }

    fn flow_statistics(&self) -> Vec<StatisticsFile> {
        self.head
            .metadata()
            .statistics_iter()
            .filter(|file| file.statistics_path.contains("/metadata/flow-stats-"))
            .cloned()
            .collect()
    }

    /// The single Flow statistics entry, on the head, and its per-field
    /// estimates from the file and from the metadata.
    async fn published(&self) -> (StatisticsFile, HashMap<i32, f64>) {
        let entries = self.flow_statistics();
        assert_eq!(entries.len(), 1, "Flow keeps one statistics entry");
        let entry = entries.into_iter().next().unwrap();
        assert_eq!(
            Some(entry.snapshot_id),
            self.head.metadata().current_snapshot_id()
        );
        let io = self.head.file_io();
        let bytes = io
            .new_input(&entry.statistics_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        assert_eq!(bytes.len() as i64, entry.file_size_in_bytes);
        // Readers seek the footer from its size: it starts with the magic.
        let footer = bytes.len() - entry.file_footer_size_in_bytes as usize;
        assert_eq!(&bytes[footer..footer + 4], b"PFA1");
        assert_eq!(&bytes[bytes.len() - 4..], b"PFA1");
        let sketches = read_sketches(io, &entry.statistics_path).await.unwrap();
        let mut estimates = HashMap::new();
        let mut sketches = sketches;
        for (field, sketch) in &mut sketches.sketches {
            let blob = entry
                .blob_metadata
                .iter()
                .find(|blob| blob.fields == [*field])
                .unwrap();
            assert_eq!(blob.r#type, "apache-datasketches-theta-v1");
            assert_eq!(blob.properties["ndv"], sketch.ndv().to_string());
            estimates.insert(*field, sketch.estimate());
        }
        (entry, estimates)
    }

    fn distinct_values(&self) -> usize {
        self.model.values().collect::<HashSet<_>>().len()
    }
}

#[tokio::test]
async fn compaction_outputs_publish_the_tables_distinct_counts() {
    let mut fixture = Fixture::new().await;
    let maintenance = fixture.maintenance(true);
    for batch in 0..4 {
        fixture
            .insert(batch * 500..(batch + 1) * 500, |id| format!("v{}", id % 97))
            .await;
    }
    // Ingest files are not sketched, so nothing covers the rows yet.
    for file in fixture.live_data_files().await {
        assert!(
            !std::path::Path::new(&sketch_path(&file)).exists(),
            "{file} is an ingest file with a sketch"
        );
    }
    let report = maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert_eq!(report.skipped, Some("coverage"));
    assert_eq!(report.covered_rows, 0);
    assert_eq!(report.total_rows, 2000);
    assert!(fixture.flow_statistics().is_empty());

    fixture.compact(&maintenance).await;
    let files = fixture.live_data_files().await;
    assert!(files.iter().all(|file| !file.contains("flow-l0-")));
    for file in &files {
        assert!(
            std::path::Path::new(&sketch_path(file)).exists(),
            "{file} has no sketch"
        );
    }
    let report = maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert!(report.published.is_some(), "{report:?}");
    assert_eq!((report.covered_rows, report.total_rows), (2000, 2000));
    assert_eq!(report.columns, 2);
    fixture.refresh().await;
    let (_, estimates) = fixture.published().await;
    assert_eq!(estimates[&1], 2000.0, "exact below the sketch size");
    assert_eq!(estimates[&2], 97.0);

    // Deletes and updates, compacted away: the union is rebuilt once its
    // rewritten files hold too many of its rows.
    let changes = (0..600)
        .map(|id| (id, Change::Delete))
        .chain((600..1000).map(|id| (id, Change::Update(row(id, &format!("w{id}"))))))
        .collect();
    fixture.cdc(changes).await;
    fixture.compact(&maintenance).await;
    let report = maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert!(report.recomputed, "{report:?}");
    assert!(report.published.is_some(), "{report:?}");
    fixture.refresh().await;
    let (_, estimates) = fixture.published().await;
    let live_rows = fixture.model.len() as f64;
    assert!(
        estimates[&1] >= live_rows && estimates[&1] <= 2000.0,
        "{estimates:?} for {live_rows} rows"
    );
    assert!(estimates[&2] >= fixture.distinct_values() as f64);
}

#[tokio::test]
async fn files_without_sketches_are_backfilled_in_bounded_visits() {
    let mut fixture = Fixture::new().await;
    for batch in 0..4 {
        fixture
            .insert(batch * 100..(batch + 1) * 100, |id| format!("v{id}"))
            .await;
    }
    // Compacted before sketching was on.
    fixture.compact(&fixture.maintenance(false)).await;
    let files = fixture.live_data_files().await;
    assert!(!files.is_empty());
    assert!(
        files
            .iter()
            .all(|file| !std::path::Path::new(&sketch_path(file)).exists())
    );
    let maintenance = TableMaintenance::new(
        fixture.index.clone(),
        fixture.catalog.clone(),
        compaction(),
        WriterConfig::default(),
    )
    .unwrap()
    .with_statistics(
        StatisticsPolicy {
            backfill_files: 1,
            ..statistics()
        },
        HashMap::from([(fixture.schema.table_id, fixture.selection())]),
    );
    let mut visits = 0;
    loop {
        visits += 1;
        assert!(visits <= files.len(), "one backfill per visit");
        let report = maintenance
            .refresh_statistics(&fixture.head, fixture.schema.table_id)
            .await
            .unwrap();
        assert!(report.backfilled <= 1);
        if report.published.is_some() {
            break;
        }
    }
    fixture.refresh().await;
    let (_, estimates) = fixture.published().await;
    assert_eq!(estimates[&1], 400.0);
    assert_eq!(estimates[&2], 400.0);
    for file in &files {
        assert!(std::path::Path::new(&sketch_path(file)).exists());
    }
}

#[tokio::test]
async fn statistics_follow_the_head_through_expiration_and_collection() {
    let mut fixture = Fixture::new().await;
    let maintenance = fixture.maintenance(true);
    for batch in 0..3 {
        fixture
            .insert(batch * 100..(batch + 1) * 100, |id| format!("v{id}"))
            .await;
    }
    fixture.compact(&maintenance).await;
    maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    fixture.refresh().await;
    let (first, _) = fixture.published().await;

    // A later commit leaves the entry on an older snapshot; re-anchoring
    // moves it to the head before that snapshot can expire.
    fixture.insert(300..310, |id| format!("v{id}")).await;
    assert!(
        maintenance
            .reanchor_statistics(&fixture.head, fixture.schema.table_id)
            .await
            .unwrap()
    );
    fixture.refresh().await;
    let (moved, _) = fixture.published().await;
    assert_eq!(moved.statistics_path, first.statistics_path);
    assert!(
        !maintenance
            .reanchor_statistics(&fixture.head, fixture.schema.table_id)
            .await
            .unwrap(),
        "already on the head"
    );

    // A newer publication replaces Flow's entry; the first file is garbage.
    fixture.compact(&maintenance).await;
    maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    fixture.refresh().await;
    let (second, _) = fixture.published().await;
    assert_ne!(second.statistics_path, first.statistics_path);

    tokio::time::sleep(Duration::from_millis(5)).await;
    maintenance
        .expire_history(
            &fixture.head,
            fixture.schema.table_id,
            &HistoryPolicy::window(Duration::from_millis(1)),
            &BTreeSet::new(),
        )
        .await
        .unwrap();
    fixture.refresh().await;
    let protection = GarbageProtection::default();
    let policy = GarbagePolicy {
        grace: Duration::from_millis(1),
        metadata_grace: Duration::from_millis(1),
        ..Default::default()
    };
    for _ in 0..4 {
        loop {
            let report = maintenance
                .collect_garbage(&fixture.head, fixture.schema.table_id, &policy, &protection)
                .await
                .unwrap();
            if !report.continuation_required {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    fixture.refresh().await;
    let (current, _) = fixture.published().await;
    assert_eq!(current.statistics_path, second.statistics_path);
    assert!(std::path::Path::new(&current.statistics_path).exists());
    assert!(
        !std::path::Path::new(&first.statistics_path).exists(),
        "a replaced statistics file is collected"
    );
    let live: HashSet<_> = fixture.live_data_files().await.into_iter().collect();
    for file in &live {
        if !file.contains("flow-l0-") {
            assert!(
                std::path::Path::new(&sketch_path(file)).exists(),
                "the sketch of live {file} was collected"
            );
        }
    }
    let data = std::path::Path::new(fixture.head.metadata().location()).join("data");
    for entry in std::fs::read_dir(data).unwrap() {
        let path = entry.unwrap().path().to_string_lossy().into_owned();
        if let Some(target) = path.strip_suffix(".sketch.puffin") {
            assert!(live.contains(target), "{path} outlived its data file");
        }
    }
}

#[tokio::test]
async fn foreign_statistics_are_left_alone() {
    let mut fixture = Fixture::new().await;
    let maintenance = fixture.maintenance(true);
    fixture.insert(0..100, |id| format!("v{id}")).await;
    fixture.insert(100..200, |id| format!("v{id}")).await;
    fixture.compact(&maintenance).await;
    // Another engine's statistics on the head, as `ANALYZE` writes.
    let head = fixture.head.metadata().current_snapshot_id().unwrap();
    let foreign = StatisticsFile {
        snapshot_id: head,
        statistics_path: format!(
            "{}/metadata/{head}-analyze.stats",
            fixture.head.metadata().location()
        ),
        file_size_in_bytes: 100,
        file_footer_size_in_bytes: 50,
        key_metadata: None,
        blob_metadata: Vec::new(),
    };
    fixture
        .catalog
        .update_table(
            TableCommit::builder()
                .ident(fixture.head.identifier().clone())
                .requirements(Vec::new())
                .updates(vec![TableUpdate::SetStatistics {
                    statistics: foreign.clone(),
                }])
                .build(),
        )
        .await
        .unwrap();
    fixture.refresh().await;
    let report = maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert_eq!(report.skipped, Some("foreign"));
    fixture.refresh().await;
    let entries: Vec<_> = fixture.head.metadata().statistics_iter().cloned().collect();
    assert_eq!(entries, [foreign]);
}

/// A field selected after files were sketched is in none of their sketch
/// files: they count toward coverage only once sketched again, never as
/// rows with no values.
#[tokio::test]
async fn a_newly_selected_column_is_backfilled_before_it_counts() {
    let mut fixture = Fixture::new().await;
    for batch in 0..3 {
        fixture
            .insert(batch * 100..(batch + 1) * 100, |id| format!("v{}", id % 37))
            .await;
    }
    // Sketched for `id` only.
    let before = fixture.sketching(&[1], statistics());
    fixture.compact(&before).await;
    let files = fixture.live_data_files().await;
    assert!(!files.is_empty());

    // `value` selected later; no file may be re-sketched yet.
    let starved = fixture.sketching(
        &[1, 2],
        StatisticsPolicy {
            backfill_files: 0,
            ..statistics()
        },
    );
    let report = starved
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert_eq!(report.skipped, Some("coverage"), "{report:?}");
    assert_eq!(report.covered_rows, 0);

    let after = fixture.sketching(&[1, 2], statistics());
    let report = after
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert!(report.published.is_some(), "{report:?}");
    assert_eq!(report.backfilled, files.len());
    fixture.refresh().await;
    let (_, estimates) = fixture.published().await;
    assert_eq!(estimates[&1], 300.0);
    assert_eq!(estimates[&2], 37.0);
    // The rewritten sketch files now hold both fields.
    for file in &files {
        let sketches = read_sketches(fixture.head.file_io(), &sketch_path(file))
            .await
            .unwrap();
        let fields: Vec<_> = sketches.sketches.iter().map(|(field, _)| *field).collect();
        assert_eq!(fields, [1, 2], "{file}");
    }
}

/// Statistics have no commit requirement: another writer's `ANALYZE` on the
/// same snapshot between Flow's check and its commit is restored, not lost.
#[tokio::test]
async fn statistics_set_concurrently_by_another_writer_are_kept() {
    let mut fixture = Fixture::new().await;
    let maintenance = fixture.maintenance(true);
    fixture.insert(0..100, |id| format!("v{id}")).await;
    fixture.insert(100..200, |id| format!("v{id}")).await;
    fixture.compact(&maintenance).await;
    let head = fixture.head.metadata().current_snapshot_id().unwrap();
    let foreign = StatisticsFile {
        snapshot_id: head,
        statistics_path: format!(
            "{}/metadata/{head}-analyze.stats",
            fixture.head.metadata().location()
        ),
        file_size_in_bytes: 100,
        file_footer_size_in_bytes: 50,
        key_metadata: None,
        blob_metadata: Vec::new(),
    };
    fixture
        .catalog
        .commit_before_next_commit(vec![TableUpdate::SetStatistics {
            statistics: foreign.clone(),
        }]);
    let report = maintenance
        .refresh_statistics(&fixture.head, fixture.schema.table_id)
        .await
        .unwrap();
    assert_eq!(report.skipped, Some("foreign"), "{report:?}");
    assert!(report.published.is_none());
    fixture.refresh().await;
    let entries: Vec<_> = fixture.head.metadata().statistics_iter().cloned().collect();
    assert_eq!(entries, [foreign]);
}

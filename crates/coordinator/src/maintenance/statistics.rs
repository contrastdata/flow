//! Table column statistics: Theta sketches of distinct values, published as
//! an Iceberg statistics file of the current snapshot.
//!
//! Data files are sketched where their rows are already in memory: as
//! compaction and the initial copy write them. Ingest (L0) files are not;
//! they reach the statistics once compaction rewrites them. A periodic visit
//! unions the sketch files of live data files, sketches a bounded number of
//! files that lack one, and publishes the union when it covers enough rows.
//! Statistics are advisory: no failure here blocks publication.
//!
//! Trino reads statistics from the queried snapshot's closest ancestor that
//! has any, and stops at the first expired one, so Flow keeps its entry on a
//! recent snapshot: [`TableMaintenance::reanchor_statistics`] moves it to the
//! head before history expiration removes the snapshot it is on.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use flow_compactor::Level;
use flow_iceberg_ext::SnapshotView;
use flow_materializer::statistics::{self, FileSketches};
use flow_materializer::{SketchSelection, sketch_path};
use flow_model::TableId;
use iceberg::spec::{DataContentType, StatisticsFile};
use iceberg::table::Table;
use iceberg::{TableCommit, TableRequirement, TableUpdate};

use super::TableMaintenance;
use super::levels::file_level;
use crate::artifacts;

/// How a table's statistics are kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatisticsPolicy {
    /// Least time between two publications of a table's statistics.
    pub refresh_interval: Duration,
    /// Publish only when sketched files hold at least this share of rows.
    pub min_coverage_percent: u8,
    /// Recompute the union once files rewritten or removed since it was built
    /// held this share of its rows: their values cannot be subtracted.
    pub recompute_percent: u8,
    /// Sketch files read in one visit.
    pub max_sketch_reads: usize,
    /// Data files without a sketch file that one visit reads to sketch.
    pub backfill_files: usize,
    /// Wait before another visit when one published nothing and had no
    /// sketch files left to read, as while compaction has yet to cover
    /// enough rows.
    pub retry_interval: Duration,
}

impl Default for StatisticsPolicy {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(900),
            min_coverage_percent: 80,
            recompute_percent: 10,
            max_sketch_reads: 64,
            backfill_files: 4,
            retry_interval: Duration::from_secs(60),
        }
    }
}

/// The columns each table sketches, and how statistics are kept.
#[derive(Debug, Default)]
pub(crate) struct TableStatistics {
    pub(crate) policy: StatisticsPolicy,
    pub(crate) selections: HashMap<TableId, Arc<SketchSelection>>,
    state: std::sync::Mutex<HashMap<TableId, SketchState>>,
}

impl TableStatistics {
    pub(crate) fn new(
        policy: StatisticsPolicy,
        selections: HashMap<TableId, Arc<SketchSelection>>,
    ) -> Self {
        Self {
            policy,
            selections,
            state: Default::default(),
        }
    }
}

/// The union of the sketch files read so far for one table incarnation.
#[derive(Debug, Clone)]
struct SketchState {
    table_uuid: uuid::Uuid,
    fields: Vec<i32>,
    union: FileSketches,
    /// Live data files in the union, with their row counts.
    files: HashMap<String, u64>,
    /// Rows of files in the union that are no longer live.
    purged_rows: u64,
    /// Files without a usable sketch file and not to be retried.
    unsketched: HashSet<String>,
    published: Option<Instant>,
    /// No visit is due before this.
    next_visit: Option<Instant>,
}

impl SketchState {
    fn new(table_uuid: uuid::Uuid, selection: &SketchSelection) -> Self {
        Self {
            table_uuid,
            fields: selection
                .columns
                .iter()
                .map(|column| column.field_id)
                .collect(),
            union: FileSketches::new(selection),
            files: HashMap::new(),
            purged_rows: 0,
            unsketched: HashSet::new(),
            published: None,
            next_visit: None,
        }
    }

    fn covered_rows(&self) -> u64 {
        self.files.values().sum()
    }

    /// Whether `sketches` hold a sketch of every field this union covers.
    fn covered_by(&self, sketches: &FileSketches) -> bool {
        self.fields
            .iter()
            .all(|field| sketches.sketches.iter().any(|(other, _)| other == field))
    }

    fn merge(&mut self, sketches: &FileSketches) {
        for (field, sketch) in &mut self.union.sketches {
            if let Some((_, other)) = sketches.sketches.iter().find(|(other, _)| other == field) {
                sketch.union(other);
            }
        }
    }
}

/// What one statistics visit did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StatisticsReport {
    /// Set when the visit published a statistics file.
    pub published: Option<String>,
    /// Why nothing was published, when that was the outcome.
    pub skipped: Option<&'static str>,
    pub columns: usize,
    pub covered_rows: u64,
    pub total_rows: u64,
    pub sketch_reads: usize,
    pub backfilled: usize,
    pub recomputed: bool,
}

/// Whether a statistics entry is one Flow publishes: a flat child of the
/// table's metadata directory named `flow-stats-*.stats`.
pub(crate) fn flow_statistics(table: &Table, statistics: &StatisticsFile) -> bool {
    is_flow_statistics(table.metadata().location(), statistics)
}

fn is_flow_statistics(location: &str, statistics: &StatisticsFile) -> bool {
    let prefix = format!("{}/metadata/flow-stats-", location.trim_end_matches('/'));
    statistics
        .statistics_path
        .strip_prefix(&prefix)
        .is_some_and(|name| name.ends_with(".stats") && !name.contains('/'))
}

/// Another writer's statistics on `snapshot_id`, if any.
fn foreign_statistics(table: &Table, snapshot_id: i64) -> Option<&StatisticsFile> {
    table
        .metadata()
        .statistics_for_snapshot(snapshot_id)
        .filter(|entry| !flow_statistics(table, entry))
}

impl TableMaintenance {
    /// Sketch `selections`' columns of each table's compaction output and
    /// initial copy, and publish their statistics under `policy`. Tables
    /// without a selection write and publish none.
    pub fn with_statistics(
        mut self,
        policy: StatisticsPolicy,
        selections: HashMap<TableId, Arc<SketchSelection>>,
    ) -> Self {
        self.statistics = Arc::new(TableStatistics::new(policy, selections));
        self
    }

    /// The columns `table_id`'s rewrites sketch, if any.
    pub(crate) fn sketch_selection(&self, table_id: TableId) -> Option<Arc<SketchSelection>> {
        self.statistics.selections.get(&table_id).cloned()
    }

    /// Whether a statistics visit is due: the table sketches columns, and
    /// either the last visit left sketch files to read or its wait is over
    /// (the refresh interval after a publication, the retry interval
    /// otherwise).
    pub fn statistics_due(&self, table_id: TableId) -> bool {
        if !self.statistics.selections.contains_key(&table_id) {
            return false;
        }
        let state = self
            .statistics
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state
            .get(&table_id)
            .is_none_or(|state| state.next_visit.is_none_or(|next| Instant::now() >= next))
    }

    /// One bounded statistics visit on the table actor.
    pub async fn refresh_statistics(
        &self,
        table: &Table,
        table_id: TableId,
    ) -> Result<StatisticsReport> {
        let mut report = StatisticsReport::default();
        let Some(selection) = self.sketch_selection(table_id) else {
            report.skipped = Some("disabled");
            return Ok(report);
        };
        let metadata = table.metadata();
        let Some(head) = metadata.current_snapshot() else {
            report.skipped = Some("empty");
            return Ok(report);
        };
        let uuid = metadata.uuid();
        let mut state = {
            let mut states = self
                .statistics
                .state
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            states
                .remove(&table_id)
                .filter(|state| {
                    state.table_uuid == uuid
                        && state.fields
                            == selection
                                .columns
                                .iter()
                                .map(|column| column.field_id)
                                .collect::<Vec<_>>()
                })
                .unwrap_or_else(|| SketchState::new(uuid, &selection))
        };
        let result = self
            .refresh_with(
                table,
                table_id,
                head.snapshot_id(),
                &selection,
                &mut state,
                &mut report,
            )
            .await;
        let policy = &self.statistics.policy;
        let now = Instant::now();
        state.next_visit = Some(if report.sketch_reads >= policy.max_sketch_reads {
            now
        } else if let Some(published) = state.published {
            (published + policy.refresh_interval)
                .max(now + policy.retry_interval.min(policy.refresh_interval))
        } else {
            now + policy.retry_interval
        });
        self.statistics
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(table_id, state);
        result.map(|()| report)
    }

    async fn refresh_with(
        &self,
        table: &Table,
        table_id: TableId,
        head: i64,
        selection: &SketchSelection,
        state: &mut SketchState,
        report: &mut StatisticsReport,
    ) -> Result<()> {
        let policy = &self.statistics.policy;
        let metadata = table.metadata();
        let view = SnapshotView::current_with_cache(table, &self.cache).await?;
        let data_prefix = format!("{}/data/", metadata.location().trim_end_matches('/'));
        let mut live = Vec::new();
        for entry in view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
        {
            let summary = entry
                .snapshot_id
                .and_then(|id| metadata.snapshot_by_id(id))
                .map(|snapshot| &snapshot.summary().additional_properties);
            let level = file_level(&data_prefix, entry.file_path(), summary);
            live.push((
                entry.file_path().to_owned(),
                entry.data_file.record_count(),
                level,
            ));
        }
        live.sort();
        report.total_rows = live.iter().map(|(_, rows, _)| rows).sum();
        // Values of rewritten files stay in the union; rebuild it once they
        // hold too large a share of the rows it covers.
        {
            let live_paths: HashSet<&str> = live.iter().map(|(path, ..)| path.as_str()).collect();
            let gone: Vec<String> = state
                .files
                .keys()
                .filter(|path| !live_paths.contains(path.as_str()))
                .cloned()
                .collect();
            for path in gone {
                state.purged_rows += state.files.remove(&path).unwrap_or(0);
            }
            state
                .unsketched
                .retain(|path| live_paths.contains(path.as_str()));
        }
        let covered = state.covered_rows();
        if state.purged_rows > 0
            && u128::from(state.purged_rows) * 100
                > u128::from(covered + state.purged_rows) * u128::from(policy.recompute_percent)
        {
            let published = state.published;
            *state = SketchState::new(state.table_uuid, selection);
            state.published = published;
            report.recomputed = true;
        }

        let io = table.file_io();
        for (path, rows, level) in &live {
            if *level == Level::L0
                || state.files.contains_key(path)
                || state.unsketched.contains(path)
            {
                continue;
            }
            if report.sketch_reads >= policy.max_sketch_reads {
                break;
            }
            report.sketch_reads += 1;
            let sidecar = sketch_path(path);
            // A file counts only once its sketches hold every selected
            // field: one sketched before a field was selected would add
            // its rows to the coverage but none of its values.
            let read = statistics::read_sketches(io, &sidecar).await;
            match read {
                Ok(sketches) if state.covered_by(&sketches) => {
                    state.merge(&sketches);
                    state.files.insert(path.clone(), *rows);
                }
                _ if report.backfilled < policy.backfill_files => {
                    // A file written before sketching or before a field was
                    // selected, or by another writer.
                    report.backfilled += 1;
                    match self
                        .backfill(table, table_id, path, &sidecar, selection)
                        .await
                    {
                        Ok(sketches) => {
                            state.merge(&sketches);
                            state.files.insert(path.clone(), *rows);
                        }
                        Err(error) => {
                            tracing::warn!(
                                event = "statistics_backfill_failed",
                                table_id = table_id.0,
                                data_file = path.as_str(),
                                %error,
                                "data file left unsketched"
                            );
                            state.unsketched.insert(path.clone());
                        }
                    }
                }
                // Retried on a later visit, within its backfill budget.
                _ => {}
            }
        }

        report.columns = state.union.sketches.len();
        report.covered_rows = state.covered_rows();
        if report.total_rows == 0 {
            report.skipped = Some("no_rows");
            return Ok(());
        }
        if u128::from(report.covered_rows) * 100
            < u128::from(report.total_rows) * u128::from(policy.min_coverage_percent)
        {
            report.skipped = Some("coverage");
            return Ok(());
        }
        if state
            .published
            .is_some_and(|at| at.elapsed() < policy.refresh_interval)
        {
            report.skipped = Some("interval");
            return Ok(());
        }
        if foreign_statistics(table, head).is_some() {
            report.skipped = Some("foreign");
            return Ok(());
        }
        let sequence = metadata
            .snapshot_by_id(head)
            .map_or(0, |snapshot| snapshot.sequence_number());
        let path = format!(
            "{}/metadata/flow-stats-{}.stats",
            metadata.location().trim_end_matches('/'),
            uuid::Uuid::new_v4()
        );
        artifacts::register_owned_path(&self.store, table, table_id, &path, "statistics").await?;
        let mut union = state.union.clone();
        union.rows = report.covered_rows;
        let sizes =
            statistics::write_sketches(io, &path, &mut union, (head, sequence), false).await?;
        let file = statistics::statistics_file(&path, sizes, &mut union, (head, sequence))?;
        if !self.commit_statistics(table, table_id, file).await? {
            // The unreferenced file is collected as garbage.
            report.skipped = Some("foreign");
            return Ok(());
        }
        state.published = Some(Instant::now());
        report.published = Some(path);
        Ok(())
    }

    /// Sketch an existing data file and keep its sketches beside it.
    async fn backfill(
        &self,
        table: &Table,
        table_id: TableId,
        path: &str,
        sidecar: &str,
        selection: &SketchSelection,
    ) -> Result<FileSketches> {
        let mut sketches = flow_compactor::sketch_file(
            table.file_io(),
            path,
            selection,
            self.writer.batch_rows.max(1),
            &self.read_limits,
        )
        .await?;
        artifacts::register_owned_path(&self.store, table, table_id, sidecar, "sketch").await?;
        // Replaces a sketch file of fewer fields, if there is one.
        statistics::write_sketches(table.file_io(), sidecar, &mut sketches, (-1, -1), true)
            .await
            .context("write a backfilled sketch file")?;
        Ok(sketches)
    }

    /// Make `file` the table's Flow statistics on its snapshot, replacing
    /// Flow's previous entry. The head must not have moved: the table actor
    /// serializes this with every other commit of the table. Returns false,
    /// leaving the table's statistics to another writer, when that writer
    /// has set statistics on the snapshot.
    ///
    /// No catalog requirement covers statistics, so a writer such as
    /// `ANALYZE` may set them on the same snapshot between the check here
    /// and the commit. The commit's predecessor metadata then holds that
    /// writer's entry, and it is restored.
    async fn commit_statistics(
        &self,
        table: &Table,
        table_id: TableId,
        file: StatisticsFile,
    ) -> Result<bool> {
        let snapshot_id = file.snapshot_id;
        let fresh = self.catalog.load_table(table.identifier()).await?;
        if foreign_statistics(&fresh, snapshot_id).is_some() {
            return Ok(false);
        }
        let metadata = fresh.metadata();
        let mut updates = Vec::new();
        for previous in metadata.statistics_iter() {
            if previous.snapshot_id != snapshot_id && flow_statistics(&fresh, previous) {
                updates.push(TableUpdate::RemoveStatistics {
                    snapshot_id: previous.snapshot_id,
                });
            }
        }
        updates.push(TableUpdate::SetStatistics { statistics: file });
        // Requirements come from the caller's view, which the published
        // statistics were computed for.
        let commit = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![
                TableRequirement::UuidMatch {
                    uuid: table.metadata().uuid(),
                },
                TableRequirement::RefSnapshotIdMatch {
                    r#ref: iceberg::spec::MAIN_BRANCH.to_owned(),
                    snapshot_id: table.metadata().current_snapshot_id(),
                },
            ])
            .updates(updates)
            .build();
        crate::register_catalog_metadata(&self.store, &fresh, table_id).await?;
        let committed = self
            .catalog
            .update_table(commit)
            .await
            .context("commit table statistics")?;
        let Some(predecessor) = committed.metadata().metadata_log().last() else {
            return Ok(true);
        };
        let replaced = iceberg::spec::TableMetadata::read_from(
            committed.file_io(),
            &predecessor.metadata_file,
        )
        .await
        .context("read the metadata a statistics commit replaced")?;
        let Some(foreign) = replaced
            .statistics_for_snapshot(snapshot_id)
            .filter(|entry| !is_flow_statistics(replaced.location(), entry))
            .cloned()
        else {
            return Ok(true);
        };
        tracing::warn!(
            event = "statistics_conflict",
            table_id = table_id.0,
            snapshot_id,
            statistics_path = foreign.statistics_path.as_str(),
            "another writer set statistics on the snapshot while Flow published; restoring them"
        );
        let restore = TableCommit::builder()
            .ident(table.identifier().clone())
            .requirements(vec![TableRequirement::UuidMatch {
                uuid: committed.metadata().uuid(),
            }])
            .updates(vec![TableUpdate::SetStatistics {
                statistics: foreign,
            }])
            .build();
        crate::register_catalog_metadata(&self.store, &committed, table_id).await?;
        self.catalog
            .update_table(restore)
            .await
            .context("restore another writer's table statistics")?;
        Ok(false)
    }

    /// Move Flow's statistics to the head snapshot, so history expiration
    /// does not remove the only snapshot readers find them on. The blobs
    /// keep the snapshot they were computed from. Returns whether it moved.
    pub async fn reanchor_statistics(&self, table: &Table, table_id: TableId) -> Result<bool> {
        if !self.statistics.selections.contains_key(&table_id) {
            return Ok(false);
        }
        let metadata = table.metadata();
        let Some(head) = metadata.current_snapshot_id() else {
            return Ok(false);
        };
        if metadata.statistics_for_snapshot(head).is_some() {
            return Ok(false);
        }
        let Some(current) = metadata
            .statistics_iter()
            .filter(|statistics| flow_statistics(table, statistics))
            .max_by_key(|statistics| {
                metadata
                    .snapshot_by_id(statistics.snapshot_id)
                    .map_or(i64::MIN, |snapshot| snapshot.sequence_number())
            })
            .cloned()
        else {
            return Ok(false);
        };
        // Either outcome changed or confirmed the table's metadata.
        self.commit_statistics(
            table,
            table_id,
            StatisticsFile {
                snapshot_id: head,
                ..current
            },
        )
        .await?;
        Ok(true)
    }
}

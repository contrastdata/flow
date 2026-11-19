//! The partition specs Flow writes in.
//!
//! The table's default spec is its target layout. Ingest epochs write an
//! unpartitioned spec, so an epoch's rows never fan out across partitions;
//! compaction and initial copies write the default spec. Deletes are written
//! in each target file's own spec and partition. See `docs/partitioning.md`.

use anyhow::{Context, Result, ensure};
use flow_iceberg_ext::{
    ManifestCache, SnapshotView, TargetPartitions, ingest_spec_id, single_unpartitioned_spec,
    unpartitioned_spec_update,
};
use flow_model::TableId;
use flow_state_store::StateStore;
use iceberg::spec::PartitionSpecRef;
use iceberg::table::Table;
use iceberg::{Catalog, TableCommit, TableRequirement, TableUpdate};

/// Add an unpartitioned spec beside a partitioned default that lacks one,
/// leaving the default unchanged. Returns the table with such a spec.
pub async fn ensure_ingest_spec(
    catalog: &dyn Catalog,
    store: &StateStore,
    table: Table,
    table_id: TableId,
) -> Result<Table> {
    if ingest_spec_id(table.metadata()).is_some() {
        return Ok(table);
    }
    let metadata = table.metadata();
    let commit = TableCommit::builder()
        .ident(table.identifier().clone())
        .requirements(vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            TableRequirement::DefaultSpecIdMatch {
                default_spec_id: metadata.default_partition_spec_id(),
            },
            TableRequirement::LastAssignedPartitionIdMatch {
                last_assigned_partition_id: metadata.last_partition_id(),
            },
        ])
        .updates(vec![TableUpdate::AddSpec {
            spec: unpartitioned_spec_update(),
        }])
        .build();
    crate::register_catalog_metadata(store, &table, table_id).await?;
    let updated = match catalog.update_table(commit).await {
        Ok(updated) => updated,
        Err(error) => {
            // A lost response or a concurrent writer may have added one.
            let refreshed = catalog.load_table(table.identifier()).await?;
            if refreshed.metadata().uuid() != metadata.uuid()
                || ingest_spec_id(refreshed.metadata()).is_none()
            {
                return Err(error).context("add an unpartitioned spec for ingest files");
            }
            refreshed
        }
    };
    ensure!(
        updated.metadata().uuid() == metadata.uuid(),
        "Iceberg target UUID changed while adding the ingest spec"
    );
    let spec_id = ingest_spec_id(updated.metadata()).context("ingest spec missing after commit")?;
    tracing::info!(
        target: "flow_events",
        event = "table_ingest_spec_added",
        table_id = table_id.0,
        spec_id,
        default_spec_id = updated.metadata().default_partition_spec_id(),
        "unpartitioned spec added for ingest files"
    );
    Ok(updated)
}

/// The spec ingest epochs write in.
pub(crate) fn ingest_spec(table: &Table) -> Result<PartitionSpecRef> {
    let spec_id = ingest_spec_id(table.metadata())
        .context("partitioned table has no unpartitioned spec for ingest files")?;
    table
        .metadata()
        .partition_spec_by_id(spec_id)
        .cloned()
        .context("ingest spec disappeared")
}

/// The spec of an initial copy, compaction output or other bulk write: the
/// table's default.
pub(crate) fn target_spec(table: &Table) -> PartitionSpecRef {
    table.metadata().default_partition_spec().clone()
}

/// Where deletes of `table`'s current snapshot belong. A table with a single
/// unpartitioned spec needs no manifest read.
pub(crate) async fn current_targets(
    table: &Table,
    cache: &ManifestCache,
) -> Result<TargetPartitions> {
    if single_unpartitioned_spec(table.metadata()) {
        return Ok(TargetPartitions::uniform(
            table.metadata().default_partition_spec_id(),
        ));
    }
    let view = SnapshotView::current_with_cache(table, cache).await?;
    Ok(TargetPartitions::of_view(table.metadata(), &view))
}

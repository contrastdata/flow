use iceberg::Result;
use iceberg::io::FileIO;
use iceberg::spec::{
    DataContentType, DataFile, FormatVersion, ManifestContentType, ManifestList, ManifestListWriter,
};
use iceberg::table::Table;

use std::collections::BTreeMap;

use crate::action::{manifest_writer, manifest_writer_at};
use crate::{CommitBase, invalid};

/// The most `-d` delete manifests a plan of `table` can need: deletes may
/// target files of any spec but the first.
pub fn plan_delete_manifests(table: &Table) -> usize {
    table
        .metadata()
        .partition_specs_iter()
        .len()
        .saturating_sub(1)
}

/// The number of partition specs `deletes` span; a plan writes one delete
/// manifest per spec.
pub fn delete_specs(deletes: &[DataFile]) -> usize {
    deletes
        .iter()
        .map(DataFile::partition_spec_id)
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

/// Durable native-Iceberg descriptions of immutable prepared data artifacts.
#[derive(Debug, Clone)]
pub struct ArtifactPlan {
    pub added_data: Vec<DataFile>,
    pub added_deletes: Vec<DataFile>,
}

/// Write native manifests and their native manifest list, with the list written
/// last as the completion marker. `path` must be a unique immutable operation
/// location. Persist the operation's CommitBase separately before publication.
///
/// A manifest holds one spec's files. Data files, which one operation writes in
/// one spec, go to `-m0.avro`; deletes of the lowest spec go to `-m1.avro`, and
/// those of each further spec to `-d0.avro`, `-d1.avro` and so on. Returns the
/// number of `-d` manifests, which [`plan_delete_manifests`] bounds in advance.
pub async fn write_artifact_plan(
    table: &Table,
    path: &str,
    added_data: &[DataFile],
    added_deletes: &[DataFile],
) -> Result<usize> {
    CommitBase::new(table).validate(table)?;
    let root = path;
    let mut writers = Vec::new();
    if let Some(first) = added_data.first() {
        let spec_id = first.partition_spec_id();
        if added_data
            .iter()
            .any(|file| file.partition_spec_id() != spec_id)
        {
            return Err(invalid("prepared data files span partition specs"));
        }
        // Conditional creates refuse to replace any artifact of an earlier attempt.
        let mut writer =
            manifest_writer(table, 0, root, 0, ManifestContentType::Data, spec_id, true)?;
        for file in added_data {
            writer.add_file(file.clone(), -1)?;
        }
        writers.push(writer);
    }
    let mut by_spec: BTreeMap<i32, Vec<&DataFile>> = BTreeMap::new();
    for file in added_deletes {
        by_spec
            .entry(file.partition_spec_id())
            .or_default()
            .push(file);
    }
    let extra = by_spec.len().saturating_sub(1);
    for (index, (spec_id, files)) in by_spec.into_iter().enumerate() {
        let mut writer = if index == 0 {
            manifest_writer(
                table,
                0,
                root,
                1,
                ManifestContentType::Deletes,
                spec_id,
                true,
            )?
        } else {
            manifest_writer_at(
                table,
                0,
                format!("{root}-d{}.avro", index - 1),
                ManifestContentType::Deletes,
                spec_id,
                true,
            )?
        };
        for file in files {
            writer.add_file(file.clone(), -1)?;
        }
        writers.push(writer);
    }
    // The list is the completion marker; its manifests can be written together.
    let manifests = futures::future::try_join_all(
        writers
            .into_iter()
            .map(|writer| writer.write_manifest_file()),
    )
    .await?;
    let output = table
        .file_io()
        .new_output(path)?
        .if_absent()
        .writer()
        .await?;
    // Prepared plans deliberately leave lineage unassigned until publication.
    let mut writer = if table.metadata().format_version() == FormatVersion::V3 {
        ManifestListWriter::v3(output, 0, None, 0, None)
    } else {
        ManifestListWriter::v2(output, 0, None, 0)
    };
    writer.add_manifests(manifests.into_iter())?;
    writer.close().await?;
    Ok(extra)
}

pub async fn read_artifact_plan(file_io: &FileIO, path: &str) -> Result<ArtifactPlan> {
    let bytes = file_io.new_input(path)?.read().await?;
    let manifests = ManifestList::parse_with_version(&bytes, FormatVersion::V3)?;
    let mut plan = ArtifactPlan {
        added_data: Vec::new(),
        added_deletes: Vec::new(),
    };
    for manifest in manifests.entries() {
        let entries = manifest.load_manifest(file_io).await?;
        for entry in entries.entries() {
            if !entry.is_alive() {
                return Err(invalid("artifact plan contains a removed entry"));
            }
            match entry.content_type() {
                DataContentType::Data => plan.added_data.push(entry.data_file.clone()),
                DataContentType::PositionDeletes => {
                    plan.added_deletes.push(entry.data_file.clone())
                }
                DataContentType::EqualityDeletes => {
                    return Err(invalid("artifact plan contains equality deletes"));
                }
            }
        }
    }
    Ok(plan)
}

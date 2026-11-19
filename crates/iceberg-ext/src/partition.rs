//! Partition specs of tables whose files span several specs.
//!
//! Flow writes ingest files in an unpartitioned spec and rewrites them into the
//! table's default spec. A position delete applies only to data files with the
//! same spec ID and partition values, so every delete file is written in its
//! target's spec and partition.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::Result;
use iceberg::spec::{
    DataContentType, DataFile, Literal, PrimitiveLiteral, Struct, TableMetadata,
    UnboundPartitionSpec,
};

use crate::{SnapshotView, invalid};

/// The spec ingest files are written in: the lowest-ID spec without fields.
/// `None` means the table needs one added; see [`unpartitioned_spec_update`].
pub fn ingest_spec_id(metadata: &TableMetadata) -> Option<i32> {
    metadata
        .partition_specs_iter()
        .filter(|spec| spec.fields().is_empty())
        .map(|spec| spec.spec_id())
        .min()
}

/// Whether every file Flow writes uses one unpartitioned spec: the table has
/// no other spec, so no file can be in another.
pub fn single_unpartitioned_spec(metadata: &TableMetadata) -> bool {
    metadata.partition_specs_iter().len() == 1
        && metadata.default_partition_spec().fields().is_empty()
}

/// An unbound unpartitioned spec to add beside a partitioned default. The
/// catalog assigns its ID; the default spec is unchanged.
pub fn unpartitioned_spec_update() -> UnboundPartitionSpec {
    UnboundPartitionSpec::builder().build()
}

/// Canonical bytes of a partition tuple, equal exactly when the tuples are.
/// Used to group files by partition; it is not an Iceberg serialization.
pub fn partition_key(partition: &Struct) -> Vec<u8> {
    let mut bytes = Vec::new();
    for value in partition.iter() {
        let Some(value) = value else {
            bytes.push(0);
            continue;
        };
        let Literal::Primitive(value) = value else {
            // Partition transforms produce primitives; nested values do not
            // occur. Encode their debug form so equal values still group.
            bytes.push(255);
            extend_bytes(&mut bytes, format!("{value:?}").as_bytes());
            continue;
        };
        match value {
            PrimitiveLiteral::Boolean(value) => bytes.extend([1, u8::from(*value)]),
            PrimitiveLiteral::Int(value) => {
                bytes.push(2);
                bytes.extend(value.to_be_bytes());
            }
            PrimitiveLiteral::Long(value) => {
                bytes.push(3);
                bytes.extend(value.to_be_bytes());
            }
            PrimitiveLiteral::Float(value) => {
                bytes.push(4);
                bytes.extend(value.0.to_bits().to_be_bytes());
            }
            PrimitiveLiteral::Double(value) => {
                bytes.push(5);
                bytes.extend(value.0.to_bits().to_be_bytes());
            }
            PrimitiveLiteral::String(value) => {
                bytes.push(6);
                extend_bytes(&mut bytes, value.as_bytes());
            }
            PrimitiveLiteral::Binary(value) => {
                bytes.push(7);
                extend_bytes(&mut bytes, value);
            }
            PrimitiveLiteral::Int128(value) => {
                bytes.push(8);
                bytes.extend(value.to_be_bytes());
            }
            PrimitiveLiteral::UInt128(value) => {
                bytes.push(9);
                bytes.extend(value.to_be_bytes());
            }
            PrimitiveLiteral::AboveMax => bytes.push(10),
            PrimitiveLiteral::BelowMin => bytes.push(11),
        }
    }
    bytes
}

fn extend_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    bytes.extend((value.len() as u64).to_be_bytes());
    bytes.extend_from_slice(value);
}

/// The spec and partition of each data file a delete may target. Delete
/// writers place each position in a file with its target's spec and partition.
#[derive(Debug, Clone)]
pub struct TargetPartitions {
    inner: Arc<Targets>,
}

#[derive(Debug)]
enum Targets {
    /// Every target is in this unpartitioned spec.
    Uniform(i32, Struct),
    Files(HashMap<String, (i32, Struct)>),
}

impl TargetPartitions {
    /// All targets are in one unpartitioned spec.
    pub fn uniform(spec_id: i32) -> Self {
        Self {
            inner: Arc::new(Targets::Uniform(spec_id, Struct::empty())),
        }
    }

    /// The live data files of `view`. A table whose only spec is
    /// unpartitioned resolves every target without a lookup.
    pub fn of_view(metadata: &TableMetadata, view: &SnapshotView) -> Self {
        if single_unpartitioned_spec(metadata) {
            return Self::uniform(metadata.default_partition_spec_id());
        }
        let files = view
            .live_files
            .values()
            .filter(|entry| entry.content_type() == DataContentType::Data)
            .map(|entry| {
                (
                    entry.file_path().to_owned(),
                    (
                        entry.data_file.partition_spec_id(),
                        entry.data_file.partition().clone(),
                    ),
                )
            })
            .collect();
        Self {
            inner: Arc::new(Targets::Files(files)),
        }
    }

    /// Also resolve `files`, such as a rewrite's outputs not yet in a snapshot.
    pub fn with_files<'a>(self, files: impl IntoIterator<Item = &'a DataFile>) -> Self {
        let Targets::Files(existing) = &*self.inner else {
            // Every file Flow writes in such a table is in its only spec.
            return self;
        };
        let mut existing = existing.clone();
        existing.extend(files.into_iter().map(|file| {
            (
                file.file_path().to_owned(),
                (file.partition_spec_id(), file.partition().clone()),
            )
        }));
        Self {
            inner: Arc::new(Targets::Files(existing)),
        }
    }

    /// Explicit targets, as in tests or for files not yet in a snapshot.
    pub fn from_files(files: impl IntoIterator<Item = (String, (i32, Struct))>) -> Self {
        Self {
            inner: Arc::new(Targets::Files(files.into_iter().collect())),
        }
    }

    /// Whether every target is in one unpartitioned spec.
    pub fn is_uniform(&self) -> bool {
        matches!(*self.inner, Targets::Uniform(..))
    }

    /// The spec ID and partition of a target data file.
    pub fn get(&self, data_file: &str) -> Result<(i32, &Struct)> {
        match &*self.inner {
            Targets::Uniform(spec_id, partition) => Ok((*spec_id, partition)),
            Targets::Files(files) => files
                .get(data_file)
                .map(|(spec_id, partition)| (*spec_id, partition))
                .ok_or_else(|| {
                    invalid(format!(
                        "delete target is not a live data file of the base snapshot: {data_file}"
                    ))
                }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partition_keys_distinguish_values_types_and_nulls() {
        let keys = [
            Struct::empty(),
            Struct::from_iter([None]),
            Struct::from_iter([Some(Literal::int(1))]),
            Struct::from_iter([Some(Literal::long(1))]),
            Struct::from_iter([Some(Literal::string("a"))]),
            Struct::from_iter([Some(Literal::string("a")), None]),
            Struct::from_iter([Some(Literal::string("a")), Some(Literal::int(0))]),
        ]
        .map(|partition| partition_key(&partition));
        for (index, key) in keys.iter().enumerate() {
            assert!(keys[index + 1..].iter().all(|other| other != key));
        }
        assert_eq!(
            partition_key(&Struct::from_iter([Some(Literal::int(7))])),
            partition_key(&Struct::from_iter([Some(Literal::int(7))]))
        );
    }
}

//! Position deletes of partitioned targets: every file is in its targets'
//! spec and partition, and the sorted positions of interleaved partitions
//! still pool into one file per partition.

use std::fs::File;

use arrow_array::{Int64Array, StringArray};
use flow_iceberg_ext::TargetPartitions;
use flow_materializer::{DeleteWriter, WriterConfig};
use flow_model::OperationId;
use iceberg::{
    io::FileIO,
    spec::{FormatVersion, Literal, Struct},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn partition(bucket: i32) -> Struct {
    Struct::from_iter([Some(Literal::int(bucket))])
}

/// Targets named so that path order alternates between two buckets, as a
/// rewrite's outputs are numbered across the partitions it writes.
fn targets(count: usize) -> (Vec<String>, TargetPartitions) {
    let paths: Vec<_> = (0..count)
        .map(|index| format!("s3://bucket/data/op-data-{index:06}.parquet"))
        .collect();
    let targets = TargetPartitions::from_files(
        paths
            .iter()
            .enumerate()
            .map(|(index, path)| (path.clone(), (1, partition((index % 2) as i32)))),
    );
    (paths, targets)
}

fn read(path: &str) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap()
    {
        let batch = batch.unwrap();
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let positions = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((paths.value(index).to_owned(), positions.value(index)));
        }
    }
    rows
}

#[tokio::test]
async fn interleaved_partitions_pool_into_one_file_each() {
    let directory = tempfile::tempdir().unwrap();
    let (paths, targets) = targets(8);
    let mut writer = DeleteWriter::new(
        FileIO::new_with_fs(),
        directory.path().to_str().unwrap(),
        &OperationId("pooled".into()),
        targets,
        WriterConfig::default(),
    )
    .unwrap();
    let positions: Vec<_> = paths
        .iter()
        .flat_map(|path| (0..3u64).map(move |position| (path.as_str(), position)))
        .collect();
    writer.write(&positions[..7]).await.unwrap();
    writer.write(&positions[7..]).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files.len(), 2, "one pooled file per bucket");
    for file in &files {
        assert_eq!(file.partition_spec_id(), 1);
        let bucket = match file.partition().fields() {
            [Some(Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(bucket)))] => *bucket,
            other => panic!("unexpected partition {other:?}"),
        };
        let rows = read(file.file_path());
        assert_eq!(rows.len(), 12);
        assert!(rows.is_sorted());
        assert!(rows.iter().all(|(path, _)| {
            let index = paths
                .iter()
                .position(|candidate| candidate == path)
                .unwrap();
            index % 2 == bucket as usize
        }));
    }
}

/// A sharing rule ends a partition's file where a target may not join it;
/// other partitions keep pooling.
#[tokio::test]
async fn the_sharing_rule_splits_within_a_partition() {
    let directory = tempfile::tempdir().unwrap();
    let (paths, targets) = targets(6);
    let alone = paths[2].clone();
    let mut writer = DeleteWriter::new(
        FileIO::new_with_fs(),
        directory.path().to_str().unwrap(),
        &OperationId("shared".into()),
        targets,
        WriterConfig::default(),
    )
    .unwrap()
    .with_sharing(move |previous, next| previous != alone && next != alone);
    let positions: Vec<_> = paths.iter().map(|path| (path.as_str(), 0u64)).collect();
    writer.write(&positions).await.unwrap();
    let files = writer.close().await.unwrap();
    // Bucket 0 holds targets 0, 2 and 4; target 2 is alone. Bucket 1 pools.
    let mut counts: Vec<_> = files
        .iter()
        .map(|file| read(file.file_path()).len())
        .collect();
    counts.sort();
    assert_eq!(counts, [1, 1, 1, 3]);
}

/// Deletion vectors are per target and carry their target's partition.
#[tokio::test]
async fn vectors_carry_their_targets_partition() {
    let directory = tempfile::tempdir().unwrap();
    let (paths, targets) = targets(4);
    let mut writer = DeleteWriter::new_for_version(
        FileIO::new_with_fs(),
        directory.path().to_str().unwrap(),
        &OperationId("vectors".into()),
        targets,
        WriterConfig::default(),
        FormatVersion::V3,
    )
    .unwrap();
    let positions: Vec<_> = paths.iter().map(|path| (path.as_str(), 5u64)).collect();
    writer.write(&positions).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files.len(), 4);
    for file in files {
        let target = file.referenced_data_file().unwrap();
        let index = paths.iter().position(|path| *path == target).unwrap();
        assert_eq!(file.partition(), &partition((index % 2) as i32));
        assert_eq!(file.partition_spec_id(), 1);
    }
}

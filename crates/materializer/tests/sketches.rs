//! Data files sketched as they are written: one sketch file beside each
//! output, its distinct counts exact for small files, and nothing extra
//! when sketching is off.

use std::sync::Arc;

use flow_materializer::{DataWriter, SketchSelection, WriterConfig, sketch_path, statistics};
use flow_model::{Column, ColumnType, OperationId, PgLsn, TableId, TableSchema, Value};
use iceberg::{
    io::FileIO,
    spec::{PartitionSpec, Transform},
};

fn schema() -> TableSchema {
    let column = |field_id, name: &str, data_type| Column {
        field_id,
        name: name.into(),
        data_type,
        nullable: true,
    };
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![
            Column {
                nullable: false,
                ..column(1, "id", ColumnType::Int64)
            },
            column(2, "status", ColumnType::String),
            column(
                3,
                "amount",
                ColumnType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            ),
            column(4, "token", ColumnType::Uuid),
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

fn rows() -> Vec<Vec<Value>> {
    (0..1000i64)
        .map(|id| {
            vec![
                Value::Int64(id),
                if id % 10 == 0 {
                    Value::Null
                } else {
                    Value::String(format!("s{}", id % 7))
                },
                Value::Decimal {
                    unscaled: (id % 50) as i128 - 25,
                    scale: 2,
                },
                Value::Uuid(((id % 300) as u128).to_be_bytes()),
            ]
        })
        .collect()
}

#[tokio::test]
async fn sketched_files_carry_exact_distinct_counts() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let io = FileIO::new_with_fs();
    let selection = Arc::new(SketchSelection::of_fields(&schema, &[1, 2, 3, 4]));
    let mut writer = DataWriter::new(
        io.clone(),
        directory.path().to_str().unwrap(),
        &OperationId("flow-l1-rewrite-sketch".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap()
    .with_sketches(selection)
    .unwrap();
    writer.write(&rows(), PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files.len(), 1);
    let mut sketches = statistics::read_sketches(&io, &sketch_path(files[0].file_path()))
        .await
        .unwrap();
    assert_eq!(sketches.rows, 1000);
    let ndv: Vec<_> = sketches
        .sketches
        .iter_mut()
        .map(|(field, sketch)| (*field, sketch.ndv()))
        .collect();
    // Nulls are not values; every column is below the nominal size, so exact.
    assert_eq!(ndv, [(1, 1000), (2, 7), (3, 50), (4, 300)]);
}

#[tokio::test]
async fn partitioned_outputs_are_sketched_per_file_and_off_writes_nothing() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let io = FileIO::new_with_fs();
    let spec = PartitionSpec::builder(flow_materializer::iceberg_schema(&schema).unwrap())
        .with_spec_id(1)
        .add_partition_field("id", "id_bucket", Transform::Bucket(4))
        .unwrap()
        .build()
        .unwrap();
    let mut writer = DataWriter::new(
        io.clone(),
        directory.path().to_str().unwrap(),
        &OperationId("flow-l1-rewrite-buckets".into()),
        schema.clone(),
        1,
        WriterConfig::default(),
    )
    .unwrap()
    .with_partition_spec(&spec)
    .unwrap()
    .with_sketches(Arc::new(SketchSelection::of_fields(&schema, &[1])))
    .unwrap();
    writer.write(&rows(), PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    assert_eq!(files.len(), 4);
    let mut total = 0;
    for file in &files {
        let mut sketches = statistics::read_sketches(&io, &sketch_path(file.file_path()))
            .await
            .unwrap();
        assert_eq!(sketches.rows, file.record_count());
        assert_eq!(sketches.sketches[0].1.ndv(), file.record_count());
        total += sketches.rows;
    }
    assert_eq!(total, 1000);

    let plain = tempfile::tempdir().unwrap();
    let mut writer = DataWriter::new(
        io.clone(),
        plain.path().to_str().unwrap(),
        &OperationId("flow-l0-plain".into()),
        schema.clone(),
        0,
        WriterConfig::default(),
    )
    .unwrap();
    writer.write(&rows(), PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    assert!(!std::path::Path::new(&sketch_path(files[0].file_path())).exists());
    let written = std::fs::read_dir(plain.path().join("data"))
        .unwrap()
        .count();
    assert_eq!(written, files.len(), "no sketch files without sketching");
}

//! Partitioned writes for transforms over every column representation Flow
//! writes: each file holds one partition, and its partition value is the
//! transform of its rows' values.

use flow_materializer::{DataWriter, WriterConfig, iceberg_schema};
use flow_model::{Column, ColumnType, OperationId, PgLsn, TableId, TableSchema, Value};
use iceberg::{
    io::FileIO,
    spec::{Literal, PartitionSpec, PrimitiveLiteral, Transform},
};

fn schema() -> TableSchema {
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns: vec![
            Column {
                field_id: 1,
                name: "id".into(),
                data_type: ColumnType::Int64,
                nullable: false,
            },
            Column {
                field_id: 2,
                name: "data".into(),
                data_type: ColumnType::Binary,
                nullable: true,
            },
        ],
        primary_key: vec![0],
        append_only: false,
    }
}

/// Binary columns are written as Arrow `LargeBinary`; `truncate` applies to
/// them as Iceberg allows.
#[tokio::test]
async fn binary_columns_partition_by_truncate() {
    let directory = tempfile::tempdir().unwrap();
    let schema = schema();
    let spec = PartitionSpec::builder(iceberg_schema(&schema).unwrap())
        .with_spec_id(1)
        .add_partition_field("data", "data_trunc", Transform::Truncate(2))
        .unwrap()
        .build()
        .unwrap();
    let mut writer = DataWriter::new(
        FileIO::new_with_fs(),
        directory.path().to_str().unwrap(),
        &OperationId("flow-l1-truncate-binary".into()),
        schema.clone(),
        1,
        WriterConfig::default(),
    )
    .unwrap()
    .with_partition_spec(&spec)
    .unwrap();
    let rows: Vec<Vec<Value>> = (0..60i64)
        .map(|id| {
            let data = match id % 3 {
                0 => Value::Binary(vec![0xab, 0xcd, id as u8]),
                1 => Value::Binary(vec![0x01]),
                _ => Value::Null,
            };
            vec![Value::Int64(id), data]
        })
        .collect();
    writer.write(&rows, PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    let mut partitions: Vec<_> = files
        .iter()
        .map(|file| {
            let value = match file.partition().fields() {
                [Some(Literal::Primitive(PrimitiveLiteral::Binary(bytes)))] => Some(bytes.clone()),
                [None] => None,
                other => panic!("unexpected partition {other:?}"),
            };
            (value, file.record_count())
        })
        .collect();
    partitions.sort();
    assert_eq!(
        partitions,
        [
            (None, 20),
            (Some(vec![0x01]), 20),
            (Some(vec![0xab, 0xcd]), 20),
        ]
    );
}

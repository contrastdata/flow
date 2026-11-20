//! Sketching an existing data file for statistics: a selected field the
//! file was written without, as one added to the table later, is null in
//! every row and gets an empty sketch instead of failing the read.

use flow_compactor::{ReadLimits, sketch_file};
use flow_materializer::{DataWriter, SketchSelection, WriterConfig};
use flow_model::{Column, ColumnType, OperationId, PgLsn, TableId, TableSchema, Value};
use iceberg::io::FileIO;

fn column(field_id: i32, name: &str, data_type: ColumnType) -> Column {
    Column {
        field_id,
        name: name.into(),
        data_type,
        nullable: true,
    }
}

fn schema(mut columns: Vec<Column>) -> TableSchema {
    columns[0].nullable = false;
    TableSchema {
        table_id: TableId(1),
        version: 0,
        columns,
        primary_key: vec![0],
        append_only: false,
    }
}

#[tokio::test]
async fn fields_the_file_lacks_have_empty_sketches() {
    let directory = tempfile::tempdir().unwrap();
    let io = FileIO::new_with_fs();
    // Written before `status` (field 2) was added.
    let before = schema(vec![column(1, "id", ColumnType::Int64)]);
    let mut writer = DataWriter::new(
        io.clone(),
        directory.path().to_str().unwrap(),
        &OperationId("flow-l1-before-add-column".into()),
        before,
        0,
        WriterConfig::default(),
    )
    .unwrap();
    let rows: Vec<Vec<Value>> = (0..500i64).map(|id| vec![Value::Int64(id)]).collect();
    writer.write(&rows, PgLsn(1)).await.unwrap();
    let files = writer.close().await.unwrap();
    let path = files[0].file_path().to_owned();

    let after = schema(vec![
        column(1, "id", ColumnType::Int64),
        column(2, "status", ColumnType::String),
    ]);
    let both = SketchSelection::of_fields(&after, &[1, 2]);
    let mut sketches = sketch_file(&io, &path, &both, 128, &ReadLimits::default())
        .await
        .unwrap();
    let ndv: Vec<_> = sketches
        .sketches
        .iter_mut()
        .map(|(field, sketch)| (*field, sketch.ndv()))
        .collect();
    assert_eq!(ndv, [(1, 500), (2, 0)]);
    assert_eq!(sketches.rows, 500);

    // None of the selected fields: every sketch is empty, nothing is read.
    let added = SketchSelection::of_fields(&after, &[2]);
    let mut sketches = sketch_file(&io, &path, &added, 128, &ReadLimits::default())
        .await
        .unwrap();
    assert_eq!(sketches.sketches.len(), 1);
    assert_eq!(sketches.sketches[0].1.ndv(), 0);
}

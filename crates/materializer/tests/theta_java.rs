//! Theta sketches against DataSketches Java, as bundled (shaded) in Iceberg's
//! Spark runtime. `fixtures/theta-java.py` regenerated `theta-java.json`:
//! run it with `spark-submit --jars iceberg-spark-runtime-3.5_2.12-1.10.1.jar`.

use flow_materializer::theta::ThetaSketch;

fn cases() -> serde_json::Map<String, serde_json::Value> {
    serde_json::from_str(include_str!("fixtures/theta-java.json")).unwrap()
}

fn java(name: &str) -> (Vec<u8>, f64) {
    let case = &cases()[name];
    let bytes = (0..case["bytes"].as_str().unwrap().len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&case["bytes"].as_str().unwrap()[at..at + 2], 16).unwrap())
        .collect();
    (bytes, case["estimate"].as_f64().unwrap())
}

fn values(name: &str) -> Vec<Vec<u8>> {
    match name {
        "empty" => vec![],
        "single" => vec![b"a".to_vec()],
        "longs_700" => (0..700i64)
            .map(|value| value.to_le_bytes().to_vec())
            .collect(),
        "strings" => ["alpha", "beta", "", "gamma", "beta", "日本"]
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect(),
        "ints_neg" => (-50..50i32)
            .map(|value| value.to_le_bytes().to_vec())
            .collect(),
        "estimation_100k" => (0..100_000i64)
            .map(|value| value.to_le_bytes().to_vec())
            .collect(),
        other => panic!("unknown case {other}"),
    }
}

fn sketch(name: &str) -> ThetaSketch {
    let mut sketch = ThetaSketch::new();
    for value in values(name) {
        sketch.update(&value);
    }
    sketch
}

/// Below the nominal size a sketch is exact, and its bytes are Java's.
#[test]
fn exact_sketches_are_byte_identical_to_java() {
    for name in ["empty", "single", "longs_700", "strings", "ints_neg"] {
        let (bytes, estimate) = java(name);
        let mut ours = sketch(name);
        assert_eq!(ours.serialize(), bytes, "{name}");
        assert_eq!(ours.estimate(), estimate, "{name}");
        assert_eq!(ThetaSketch::deserialize(&bytes).unwrap().serialize(), bytes);
    }
}

/// In estimation mode the retained entries depend on when each side
/// rebuilt, but every hash below theta agrees: Java's sketch reads back with
/// Java's estimate, and a union with ours over the same values stays one
/// estimate of the same set, not two.
#[test]
fn estimation_sketches_agree_with_java() {
    let (bytes, estimate) = java("estimation_100k");
    let mut read = ThetaSketch::deserialize(&bytes).unwrap();
    assert!(
        (read.estimate() - estimate).abs() < 1e-6,
        "{}",
        read.estimate()
    );
    let mut ours = sketch("estimation_100k");
    let bound = 3.0 / 4096f64.sqrt();
    assert!((ours.estimate() - 100_000.0).abs() / 100_000.0 < bound);
    read.union(&ours);
    let union = read.estimate();
    assert!(
        (union - 100_000.0).abs() / 100_000.0 < bound,
        "a hash mismatch would double the union: {union}"
    );
    let mut reread = ThetaSketch::deserialize(&ours.serialize()).unwrap();
    assert_eq!(reread.estimate(), ours.estimate());
}

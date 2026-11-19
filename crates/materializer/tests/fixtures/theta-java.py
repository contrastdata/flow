import json, struct, uuid
from decimal import Decimal
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
jvm = spark._jvm
theta = jvm.org.apache.iceberg.shaded.org.apache.datasketches.theta
cases = {
    "empty": [],
    "single": [b"a"],
    "longs_700": [struct.pack("<q", i) for i in range(700)],
    "strings": [s.encode() for s in ["alpha", "beta", "", "gamma", "beta", "日本"]],
    "ints_neg": [struct.pack("<i", i) for i in range(-50, 50)],
    "estimation_100k": [struct.pack("<q", i) for i in range(100000)],
}
out = {}
for name, values in cases.items():
    sketch = theta.UpdateSketch.builder().build()
    for v in values:
        sketch.update(bytearray(v))
    compact = sketch.compact()
    out[name] = {"bytes": bytes(compact.toByteArray()).hex(), "estimate": compact.getEstimate(),
                 "retained": compact.getRetainedEntries(), "theta": compact.getThetaLong()}
# Iceberg's own ndv helper over one column, to pin single-value serialization.
print("GOLDEN=" + json.dumps(out), flush=True)
spark.stop()

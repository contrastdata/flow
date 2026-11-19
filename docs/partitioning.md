# Partitioned tables

A table's Iceberg partition spec says how its data files are split by value,
so that readers can skip whole partitions. Flow writes partitioned tables
without slowing ingestion: change epochs stay unpartitioned, and compaction
moves their rows into the table's partitions. Partitioning is a layout
property only: deletes always follow a row to the file it is in, so no choice
of spec can lose or resurrect a row.

## Declaring a spec

Flow never chooses a partition spec. A table is partitioned only when its
spec is declared, in the table's `[[tables]]` block or from SQL (`ALTER TABLE
… ADD PARTITION FIELD` in Spark, `partitioning` in Trino):

```toml
[[tables]]
target_table = "orders"
primary_key = [0]
partition = ["month(created_at)"]         # or ["day(created_at)", "bucket(16, tenant_id)"]
# partition = "none"                       # unpartitioned
# partition_convert = "background"         # see below; default "lazy"
```

A field is a column (identity), `year(column)`, `month(column)`,
`day(column)`, `hour(column)`, `bucket(N, column)` or `truncate(W, column)`.
Field names follow Iceberg's convention (`created_at_month`,
`tenant_id_bucket`).

| `partition` | New table | Existing table |
| --- | --- | --- |
| absent | Unpartitioned | Its own spec, which SQL engines may change |
| set | Created unpartitioned, then given that spec at startup | Replaced at startup if it differs, with a `table_partition_spec_set` event |

Use `partition` when the deployment is managed as configuration; leave it
absent when the table's spec is managed from SQL. Flow reloads table metadata
before every commit and rewrite, so a spec changed from SQL applies from the
next epoch.

Choose a spec whose partitions each hold at least a few hundred megabytes. A
partition is the unit readers skip and compaction merges within; many small
partitions mean many small files.

## How files are written

The table's default spec is its target layout. Each kind of write uses one
spec:

| Files | Spec |
| --- | --- |
| A CDC epoch (L0) | An unpartitioned spec of the table |
| The PostgreSQL initial copy | The default spec, split by partition |
| Compaction output | The default spec, split by partition |
| Position deletes and deletion vectors | The spec and partition of the data file they apply to |

An epoch's rows therefore never fan out into one file per partition, and the
L0 limits that pause publication count the same files as for an unpartitioned
table. A table whose spec is partitioned keeps an unpartitioned spec for its
epochs: one it already has, or one Flow adds without changing the default
(`table_ingest_spec_added`). Iceberg allows files of every spec a table has;
this is the same state as after any partition evolution.

Compaction of L0 files writes one output file per partition the rows fall
in, with each file sorted in the table's sort order. A file in the default
spec holds rows of one partition only. An update that changes a row's
partition is a delete in the old file's partition and an insert into L0.

Iceberg applies a position delete only to data files with the same spec and
partition. Flow writes each delete in its target's spec and partition: an
epoch's positions on the small files of one partition pool into one delete
file, and with `delete_layout = "per_target"` a pooled range ends only at a
full-size file of the same partition.

Small-file and shared delete-file debt are counted per partition: the
partition with the most sets `flow_table_small_files` and the shared
delete-file count. Small-file rewrites merge files of one partition, and
consolidation merges the delete files of the partition with the most first. A
partition whose data fits one file never counts as small-file debt. Rewrites
selected for deleted rows, delete files or a spec change may take files of
several partitions, since their output is split by partition anyway.

## Changing a spec

Changing the spec is a metadata change. Existing files keep their spec, and
every later rewrite writes the new one. Files outside L0 in another spec are
moved:

- `partition_convert = "lazy"` (the default): when compaction rewrites them
  for its own reasons, such as deleted rows or small files.
- `partition_convert = "background"`: also by rewrites selected only for
  their spec, after every other kind of maintenance, within
  `[compaction] partition_convert_bytes_per_hour` per table (4 GiB by
  default). The plan reason is `partition_conversion`.

Readers skip partitions only among files in a spec that has the filtered
field. Until older files are moved, queries read them as before.

A conversion rewrite splits each input group across the partitions its rows
fall in. When a spec has many more partitions than the group holds files, its
outputs are small, and small-file rewrites merge them within each partition
later.

## Readers

Iceberg readers plan each file with its own spec, so a table with files in
several specs reads like any table after partition evolution. The service
suite reads partitioned Flow targets with Spark, Trino and DuckDB through CDC,
native compaction and Spark's rewrite procedures
(`tests/production/readers.py --partitioned`), and partitions a running
target from Spark before doing so (`--sql-partition`).

Recent changes stay in L0 until compaction, so a query on the newest data
cannot skip their files by partition; file statistics still prune them.
Engines that plan joins by partition, such as Spark's storage-partitioned
join, use it only when every scanned file is in a spec with those fields.

## Inspecting a table

`embrasure-flow layout` shows the default spec, the number of partitions, the
most files outside L0 in one partition and the bytes still in an earlier
spec:

```
replicated.orders  (format v2)
  sort order   id ASC NULLS FIRST  (order 1)
  partition    month(created_at)  (spec 1)
  partitions   36, up to 3 files outside L0 each; 0.00 GiB in an earlier spec
  files        112 data files, 9200000 rows: L0 4 · L1 61 · L2 47
```

Periodic maintenance exports `flow_table_partition_spec_id`,
`flow_table_partitions`, `flow_table_partition_max_files` and
`flow_table_off_spec_bytes`.

## Limits

- The ingest spec must be unpartitioned. Flow does not partition epochs at
  ingest.
- Small partitions under heavy updates cost publication latency: an epoch
  writes deletes in every partition it touches, and each partition's small
  files are rewritten to drop them. On one 8-vCPU host at 30,000 mutations/s
  (90% updates), p95 commit-to-catalog latency was 0.71 s for a 2M-row table
  in `bucket(4, id)` against 0.59 s unpartitioned, and 2.2 s for a 100k-row
  table in `bucket(16, id)` (1.5 MB partitions) against 0.52 s. CDC service
  time per mutation stayed within 4%.
- A write splits across at most 64 open files at once; a write spanning more
  partitions closes the least recently written file and starts another,
  writing more, smaller files. The initial copy of a table whose primary key
  order is unrelated to a spec with many partitions writes small files that
  compaction then merges.
- The partition source columns must be replicated columns. Flow writes the
  standard transforms; `void` and multi-argument transforms are not
  supported.
- Iceberg v1 tables are not supported, as before.

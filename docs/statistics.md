# Table statistics

Query planners estimate join and aggregation sizes from the number of
distinct values (NDV) in each column. Iceberg keeps these in statistics
files: Puffin files with one `apache-datasketches-theta-v1` blob per column,
listed in table metadata against a snapshot. Trino reads them for every
query; Spark reads them with cost-based optimization enabled. Without them,
planners guess from row counts and file bounds.

Flow can keep a statistics file current for its targets. It is off by
default.

```toml
[statistics]
enabled = true
# columns = "auto"                 # or "none", or ["customer_id", "status"]

[[tables]]
target_table = "orders"
# statistics = ["customer_id", "status"]   # this table's columns; "none" opts out
```

## What is sketched

A Theta sketch holds a sample of up to 4,096 hashed values per column. It is
exact below that and within about 3% at 95% confidence above, whatever the
table's size, and two sketches combine into the sketch of both files' rows.
Values are hashed in Iceberg's single-value binary form, so Flow's sketches
are the ones Spark's `compute_table_stats` and Trino's `ANALYZE` write; any
engine that reads those reads Flow's.

`columns = "auto"` selects the primary-key columns, then integer, date,
timestamp, UUID, decimal and string columns in schema order, up to
`max_columns` (16). JSON and JSONB columns replicate as strings and are
selected with them. Floating-point, boolean and binary columns are not
selected; list them by name to sketch them.

## How it works

Sketches are built where rows are already in memory, as files are written:

| Files | Sketched |
| --- | --- |
| CDC epochs (L0) | No |
| The PostgreSQL initial copy | Yes |
| Compaction output | Yes |

Each sketched data file gets a sidecar Puffin file beside it,
`<data file>.sketch.puffin`. Change epochs pay nothing: their rows reach the
statistics once compaction rewrites them, usually within minutes.

Periodic table maintenance then:

1. Unions the sidecars of live files outside L0 that it has not read yet, at
   most `max_sketch_reads` (64) per visit, and reads a few files that have no
   sidecar, such as files written before statistics were enabled or by
   another writer, to sketch them (`backfill_files`, 4 per visit).
2. Publishes the union as `metadata/flow-stats-<uuid>.stats` on the head
   snapshot when the sketched files hold at least `min_coverage_percent`
   (80%) of the table's rows and the last publication is at least
   `refresh_interval_secs` (900) old. The commit replaces Flow's previous
   entry and requires the head not to have moved.

A sketch cannot forget values. Once rewritten or removed files held
`recompute_percent` (10%) of the union's rows, Flow rebuilds it from the
live files' sidecars; until then, values deleted since the last rebuild
still count. Readers keep the previous statistics while it does.

A file counts toward coverage only when its sidecar holds every selected
column. After a column is added to `columns`, files sketched before hold
none of its values, so they count again only once maintenance re-sketches
them, `backfill_files` per visit; until enough have been, the previous
statistics stay published.

Statistics are advisory. A sketch that fails to write leaves its data file
unsketched, a failed visit backs off like other maintenance, and neither
delays publication.

## Readers

Trino uses the statistics file of the queried snapshot or, failing that, of
its closest ancestor that has one, and stops at the first ancestor that has
expired. Before history expiration, Flow moves its entry to the head so that
it never ends up on an expired snapshot; the blobs keep the snapshot they
were computed from. Spark looks only at the queried snapshot, so between
Flow's publications it falls back to its own estimates for the newest
snapshots.

The service suite reads Flow's statistics with Trino `SHOW STATS` and checks
them against exact counts (`tests/production/readers.py --statistics`).

## Statistics from other writers

Flow publishes only files named `metadata/flow-stats-*.stats` and leaves
another writer's entry in place. If the head already has statistics from
another writer, such as `ANALYZE`, Flow skips that publication
(`reason = "foreign"`). No catalog requirement covers statistics, so another
writer may set them on the same snapshot while Flow commits. Flow then finds
that writer's entry in the metadata its commit replaced, restores it and
logs `statistics_conflict`. Garbage collection keeps every statistics and
partition statistics file the table metadata lists, and a sidecar for as long
as its data file is retained.

## Inspecting a table

`contrast-flow layout` shows the statistics readers of the head use:

```
  statistics   Flow, 3 commits behind the head: id ≈9200000 · customer_id ≈48211 · status ≈6
```

A publication logs `statistics_published` at info level with the rows
covered and the work done. Other visits log `statistics_skipped` on the
`flow_events` target at debug level with a `reason`: `empty`, `no_rows`,
`coverage`, `interval` or `foreign`. Metrics:

| Metric | Meaning |
| --- | --- |
| `flow_statistics_published_total` | Statistics files published, by table |
| `flow_statistics_coverage_ratio` | Share of live rows in sketched files at the last visit |
| `flow_statistics_sketch_reads_total`, `flow_statistics_backfilled_files_total` | Sidecars read and data files sketched by visits |
| `flow_statistics_sketch_write_failures_total` | Data files left unsketched because their sketch failed |

## Settings

| `[statistics]` | Default | |
| --- | --- | --- |
| `enabled` | `false` | Sketch and publish statistics |
| `columns` | `"auto"` | Columns of tables without their own `statistics` |
| `max_columns` | 16 | Most columns `"auto"` selects |
| `refresh_interval_secs` | 900 | Least time between publications of one table |
| `min_coverage_percent` | 80 | Least share of rows in sketched files to publish |
| `recompute_percent` | 10 | Share of rewritten rows that triggers a rebuild |
| `max_sketch_reads` | 64 | Sidecars one visit reads |
| `backfill_files` | 4 | Unsketched data files one visit sketches |

## Rollback

An engine older than this feature does not know statistics files: its
garbage collection may delete Flow's current statistics file and the
sidecars, leaving an entry readers fail to open. Before rolling back, set
`enabled = false` and remove Flow's entries, with Trino's `ALTER TABLE ...
EXECUTE drop_extended_stats` or Iceberg's `UpdateStatistics` API. Sidecars
the older engine deletes are only rebuilt cost.

## Limits

- Only NDV. Flow does not write partition statistics or histograms; Iceberg
  manifests already carry null counts and bounds.
- Rows in L0 files are not counted until compaction rewrites them. Under
  `min_coverage_percent`, a table whose compaction falls far behind
  publishes nothing.
- Memory: a writer holds up to 64 KiB per sketched column for each file it
  has open, as Java's sketches do; maintenance keeps up to 32 KiB per
  sketched column for each table.
- A column whose type is promoted (`int` to `bigint`) hashes differently
  before and after; its estimate may double-count until the next rebuild.

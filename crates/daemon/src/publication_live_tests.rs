//! The real daemon against live PostgreSQL and an in-memory Iceberg catalog:
//! a publication change affecting one configured table blocks only that table,
//! a streamed transaction blocks a table only if it commits, and another
//! writer's row changes block only their table until rollback or a resync.
use crate::config::Config;
use flow_pg_source::tokio_postgres::{self, Client, NoTls};
use futures::FutureExt;
use iceberg::{
    CatalogBuilder,
    memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder},
};
use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

async fn lsn(sql: &Client, query: &str, slot: &str) -> i64 {
    let rows = sql.query(query, &[&slot]).await.unwrap();
    rows.first().and_then(|row| row.get(0)).unwrap_or(0)
}

async fn current(sql: &Client) -> i64 {
    sql.query_one("SELECT (pg_current_wal_lsn() - '0/0')::bigint", &[])
        .await
        .unwrap()
        .get(0)
}

async fn confirmed(sql: &Client, slot: &str) -> i64 {
    lsn(
        sql,
        "SELECT (confirmed_flush_lsn - '0/0')::bigint FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
        slot,
    )
    .await
}

fn status(config: &Config) -> serde_json::Value {
    std::fs::read(config.state_dir.join("status.json"))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
        .unwrap_or_default()
}

fn materialized(config: &Config, table: &str) -> u64 {
    status(config)["table_progress"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|progress| progress["source_table"] == table)
        .and_then(|progress| progress["materialized_lsn"].as_u64())
        .unwrap_or(0)
}

async fn until<F: Future<Output = bool>>(description: &str, mut check: impl FnMut() -> F) {
    let deadline = Instant::now() + Duration::from_secs(120);
    while !check().await {
        assert!(Instant::now() < deadline, "timed out: {description}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Run `checks` against a live daemon capturing `{name}.orders` and
/// `{name}.items`, then release its slot and drop its objects.
async fn with_daemon<F, Fut>(url: &str, sql: &Client, label: &str, checks: F)
where
    F: FnOnce(Config, String) -> Fut,
    Fut: Future<Output = ()>,
{
    with_configured_daemon(url, sql, label, |_| {}, checks).await;
}

async fn with_configured_daemon<F, Fut>(
    url: &str,
    sql: &Client,
    label: &str,
    configure: impl FnOnce(&mut Config),
    checks: F,
) where
    F: FnOnce(Config, String) -> Fut,
    Fut: Future<Output = ()>,
{
    with_seeded_daemon(url, sql, label, "", configure, checks).await;
}

/// [`with_configured_daemon`], running `seed` (with `{name}` replaced by the
/// schema) after the tables are created and before the initial copy.
async fn with_seeded_daemon<F, Fut>(
    url: &str,
    sql: &Client,
    label: &str,
    seed: &str,
    configure: impl FnOnce(&mut Config),
    checks: F,
) where
    F: FnOnce(Config, String) -> Fut,
    Fut: Future<Output = ()>,
{
    with_source(
        url,
        sql,
        label,
        seed,
        configure,
        |config, name| async move {
            crate::bootstrap::initialize(config.clone()).await.unwrap();
            let daemon = crate::runtime::run(config.clone(), false);
            tokio::pin!(daemon);
            tokio::select! {
                result = &mut daemon => panic!("daemon stopped: {result:?}"),
                () = checks(config.clone(), name.clone()) => {}
            }
        },
    )
    .await;
}

/// A seeded source schema, publication and catalog for `body`, which runs the
/// daemon itself; everything is removed afterwards.
async fn with_source<F, Fut>(
    url: &str,
    sql: &Client,
    label: &str,
    seed: &str,
    configure: impl FnOnce(&mut Config),
    body: F,
) where
    F: FnOnce(Config, String) -> Fut,
    Fut: Future<Output = ()>,
{
    let name = format!(
        "flow_public_block_{label}_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    sql.batch_execute(&format!(
        "CREATE SCHEMA {name};
         CREATE TABLE {name}.orders (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.orders REPLICA IDENTITY FULL;
         CREATE TABLE {name}.items (id bigint PRIMARY KEY, status text);
         ALTER TABLE {name}.items REPLICA IDENTITY FULL;
         INSERT INTO {name}.orders VALUES (1, 'seed');
         INSERT INTO {name}.items VALUES (1, 'seed');
         CREATE PUBLICATION {name} FOR TABLE {name}.orders, {name}.items;"
    ))
    .await
    .unwrap();
    if !seed.is_empty() {
        sql.batch_execute(&seed.replace("{name}", &name))
            .await
            .unwrap();
    }
    // Surface capture warnings (disconnects, deferred checks) in test output.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            std::env::var("FLOW_TEST_LOG").unwrap_or_else(|_| "warn".into()),
        ))
        .with_test_writer()
        .try_init();
    let root = tempfile::tempdir().unwrap();
    let mut config: Config = toml::from_str(include_str!("../../../examples/flow.toml")).unwrap();
    config.state_dir = root.path().join("state");
    // Compaction is off here, so an L0 age limit would pause publication once a
    // slow runner takes longer than it; these tests do not exercise that.
    config.compaction.oldest_l0_soft_ms = 3_600_000;
    config.compaction.oldest_l0_hard_ms = 7_200_000;
    config.source.id = name.clone();
    config.source.connection_env = "FLOW_POSTGRES_URL".into();
    config.source.slot = name.clone();
    config.source.publication = name.clone();
    // Functional tests must not depend on the host's free disk space.
    config.storage.min_free_bytes = 0;
    let uri = format!("memory://{name}");
    config.catalog = HashMap::from([("uri".into(), uri.clone())]);
    let orders = config.tables[0].clone();
    config.tables = ["orders", "items"]
        .into_iter()
        .map(|table| {
            let mut configured = orders.clone();
            configured.source_namespace = name.clone();
            configured.source_table = table.into();
            configured.target_namespace = vec![name.clone()];
            configured.target_table = table.into();
            configured
        })
        .collect();
    configure(&mut config);
    let catalog: Arc<dyn iceberg::Catalog> = Arc::new(
        MemoryCatalogBuilder::default()
            .load(
                &name,
                HashMap::from([(MEMORY_CATALOG_WAREHOUSE.into(), uri.clone())]),
            )
            .await
            .unwrap(),
    );
    crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .insert(uri.clone(), catalog);
    assert!(url.starts_with("postgres"));

    let result = AssertUnwindSafe(body(config.clone(), name.clone()))
        .catch_unwind()
        .await;

    // Dropping the daemon closes capture; always release the slot.
    for _ in 0..100 {
        sql.execute(
            "SELECT pg_catalog.pg_terminate_backend(active_pid) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND active",
            &[&name],
        )
        .await
        .unwrap();
        let _ = sql
            .query(
                "SELECT pg_catalog.pg_drop_replication_slot(slot_name) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1 AND NOT active",
                &[&name],
            )
            .await;
        let remaining: i64 = sql
            .query_one(
                "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                &[&name],
            )
            .await
            .unwrap()
            .get(0);
        if remaining == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    sql.batch_execute(&format!(
        "DROP PUBLICATION IF EXISTS {name}; DROP SCHEMA {name} CASCADE"
    ))
    .await
    .unwrap();
    crate::services::TEST_CATALOGS.lock().unwrap().remove(&uri);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

/// `change` applies a publication edit to `{name}.items`.
async fn scenario(url: &str, sql: &Client, label: &str, change: &str) {
    with_daemon(url, sql, label, |config, name| async move {
            sql.batch_execute(&format!(
                "BEGIN; INSERT INTO {name}.orders VALUES (2, 'before'); INSERT INTO {name}.items VALUES (2, 'before'); COMMIT"
            ))
            .await
            .unwrap();
            until("both tables publish before the change", || async {
                materialized(&config, "orders") > 0 && materialized(&config, "items") > 0
            })
            .await;

            sql.batch_execute(&change.replace("{name}", &name))
                .await
                .unwrap();
            // Changes that may be skipped or filtered, and a healthy-table write.
            sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (3, 'after')"))
                .await
                .unwrap();
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'after')"))
                .await
                .unwrap();
            until(
                "the changed table is blocked with publication_changed",
                || async {
                    status(&config)["blocked_tables"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .any(|record| record["error_code"] == "publication_changed")
                },
            )
            .await;
            let blocked = status(&config)["blocked_tables"].clone();
            assert_eq!(blocked.as_array().unwrap().len(), 1, "{blocked}");

            // The healthy table keeps publishing and the slot keeps advancing,
            // while the blocked table's changes keep arriving (row filter) or stop.
            let orders_before = materialized(&config, "orders");
            let mark = current(sql).await;
            for id in 10..15 {
                sql.batch_execute(&format!(
                    "INSERT INTO {name}.items VALUES ({id}, 'dropped'); INSERT INTO {name}.orders VALUES ({id}, 'kept')"
                ))
                .await
                .unwrap();
            }
            until("the healthy table keeps publishing", || async {
                materialized(&config, "orders") > orders_before
            })
            .await;
            // Freshness monitoring derives from this global watermark.
            until(
                "status watermarks.materialized_lsn advances past the blocked table",
                || async {
                    status(&config)["watermarks"]["materialized_lsn"]
                        .as_i64()
                        .is_some_and(|lsn| lsn > mark)
                },
            )
            .await;
            until(
                "confirmed_flush_lsn advances past the blocked table",
                || async { confirmed(sql, &name).await > mark },
            )
            .await;

            let status = status(&config);
            assert_ne!(status["source_health"], "publication_changed", "{status}");
            assert!(
                !config
                    .state_dir
                    .join("publication-resync-required.json")
                    .exists()
            );
            let progress = status["table_progress"].as_array().unwrap();
            assert_eq!(progress.len(), 2, "blocked tables stay in table_progress");
            for table in ["orders", "items"] {
                assert!(
                    progress
                        .iter()
                        .any(|record| record["source_namespace"] == name.as_str()
                            && record["source_table"] == table)
                );
            }
            let items = progress
                .iter()
                .find(|record| record["source_table"] == "items")
                .unwrap();
            assert_eq!(blocked[0]["table_id"], items["table_id"]);
            assert_eq!(status["state"], "running");

            // The block is visible to Prometheus alerts without parsing status.
            let series = format!(
                "flow_table_blocked{{table_id=\"{}\",code=\"publication_changed\"}} 1\n",
                items["table_id"]
            );
            until("the blocked table is exported as a gauge", || async {
                std::fs::read_to_string(config.state_dir.join("metrics.prom")).is_ok_and(
                    |metrics| {
                        metrics.contains(&series) && metrics.contains("flow_blocked_tables 1\n")
                    },
                )
            })
            .await;
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_publication_change_blocks_only_the_affected_table() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    // Removed: pgoutput stops sending the table's changes.
    scenario(
        &url,
        &sql,
        "removed",
        "ALTER PUBLICATION {name} SET TABLE {name}.orders",
    )
    .await;
    // Filtered: changes keep arriving, so they must be dropped, not retained.
    scenario(
        &url,
        &sql,
        "filtered",
        "ALTER PUBLICATION {name} SET TABLE {name}.orders, {name}.items WHERE (id > 0)",
    )
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

fn blocked_tables(config: &Config) -> Vec<serde_json::Value> {
    status(config)["blocked_tables"]
        .as_array()
        .cloned()
        .unwrap_or_default()
}

/// Every published `(id, status)` of a target table, sorted.
async fn published_values(name: &str, table: &str) -> Vec<(i64, Option<String>)> {
    use arrow_array::{Array, Int64Array, StringArray};
    use futures::TryStreamExt;
    let catalog = crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .get(&format!("memory://{name}"))
        .cloned()
        .unwrap();
    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(name.into()),
            table.into(),
        ))
        .await
        .unwrap();
    let mut batches = table.scan().build().unwrap().to_arrow().await.unwrap();
    let mut values = Vec::new();
    while let Some(batch) = batches.try_next().await.unwrap() {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let statuses = batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        for row in 0..batch.num_rows() {
            values.push((
                ids.value(row),
                (!statuses.is_null(row)).then(|| statuses.value(row).to_owned()),
            ));
        }
    }
    values.sort();
    values
}

async fn published_rows(name: &str, table: &str) -> usize {
    use futures::TryStreamExt;
    let catalog = crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .get(&format!("memory://{name}"))
        .cloned()
        .unwrap();
    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(name.into()),
            table.into(),
        ))
        .await
        .unwrap();
    let mut batches = table.scan().build().unwrap().to_arrow().await.unwrap();
    let mut rows = 0;
    while let Some(batch) = batches.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    rows
}

/// Wait until the slot's walsender decoded all WAL written so far, including
/// an open transaction's changes. A committed message flushes that WAL.
async fn decoded(sql: &Client, slot: &str, config: &Config) {
    sql.batch_execute("SELECT pg_catalog.pg_logical_emit_message(true, 'flow-test', '')")
        .await
        .unwrap();
    let mark = current(sql).await;
    let deadline = Instant::now() + Duration::from_secs(120);
    while lsn(
        sql,
        "SELECT (r.sent_lsn - '0/0')::bigint FROM pg_catalog.pg_stat_replication r JOIN pg_catalog.pg_replication_slots s ON s.active_pid = r.pid WHERE s.slot_name = $1",
        slot,
    )
    .await
        < mark
    {
        if Instant::now() >= deadline {
            // Tell a disconnected capture from one that stopped reading.
            let diagnostics = sql
                .query(
                    "SELECT format('slot active=%s pid=%s confirmed=%s wal_status=%s; sender state=%s sent=%s write=%s flush=%s; stream_txns=%s spill_txns=%s; mark=%s',
                        s.active, s.active_pid, s.confirmed_flush_lsn, s.wal_status, r.state, r.sent_lsn, r.write_lsn, r.flush_lsn,
                        st.stream_txns, st.spill_txns, pg_catalog.pg_current_wal_lsn())
                     FROM pg_catalog.pg_replication_slots s
                     LEFT JOIN pg_catalog.pg_stat_replication r ON r.pid = s.active_pid
                     LEFT JOIN pg_catalog.pg_stat_replication_slots st ON st.slot_name = s.slot_name
                     WHERE s.slot_name = $1",
                    &[&slot],
                )
                .await
                .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>());
            let sessions = sql
                .query(
                    "SELECT format('%s %s wait=%s:%s xact_age=%s query=%s', backend_type, state, wait_event_type, wait_event,
                        now() - xact_start, left(regexp_replace(query, '\\s+', ' ', 'g'), 160))
                     FROM pg_catalog.pg_stat_activity WHERE pid <> pg_backend_pid() AND state <> 'idle'",
                    &[],
                )
                .await
                .map(|rows| rows.iter().map(|row| row.get::<_, String>(0)).collect::<Vec<_>>());
            let daemon = status(config);
            panic!(
                "timed out: the walsender decodes the open transaction; {diagnostics:?}; sessions: {sessions:?}; daemon state={} source_health={} last_error={} blocked={}",
                daemon["state"], daemon["source_health"], daemon["last_error"], daemon["blocked_tables"]
            );
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn streamed_transactions(sql: &Client, slot: &str) -> i64 {
    lsn(
        sql,
        "SELECT stream_txns FROM pg_catalog.pg_stat_replication_slots WHERE slot_name = $1",
        slot,
    )
    .await
}

/// A long transaction holding ACCESS EXCLUSIVE on a published table must not
/// stall capture: the publication check backs off instead of disconnecting.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_locked_table_does_not_stall_capture() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    let held_url = url.clone();
    with_daemon(&url, sql, "locked", |config, name| async move {
        // The initial copy already published orders; prove capture is streaming
        // before the lock, or its setup would wait for the lock to end.
        // Taken before the probe so unrelated WAL cannot outrun it.
        let mark = current(sql).await;
        sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (2, 'before')"))
            .await
            .unwrap();
        until(
            "orders publishes through capture before the lock",
            || async { materialized(&config, "orders") as i64 > mark },
        )
        .await;
        let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
        let held_task = tokio::spawn(connection);
        held.batch_execute(&format!(
            "BEGIN; LOCK TABLE {name}.items IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .unwrap();
        // Longer than a source operation deadline and many check intervals:
        // every change to the unlocked table still publishes. Probes are spaced
        // out so tiny commits don't build reader debt (compaction is off here).
        let started = Instant::now();
        let mut id = 100;
        while started.elapsed() < Duration::from_secs(36) {
            let before = materialized(&config, "orders");
            sql.batch_execute(&format!(
                "INSERT INTO {name}.orders VALUES ({id}, 'locked')"
            ))
            .await
            .unwrap();
            id += 1;
            until("orders keeps publishing while items is locked", || async {
                materialized(&config, "orders") > before
            })
            .await;
            let active: bool = sql
                .query_one(
                    "SELECT active FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
                    &[&name],
                )
                .await
                .unwrap()
                .get(0);
            assert!(active, "capture disconnected while items was locked");
            tokio::time::sleep(Duration::from_secs(12)).await;
        }
        held.batch_execute("ROLLBACK").await.unwrap();
        drop(held);
        held_task.await.unwrap().unwrap();
        // Reader-debt pauses are expected here (compaction is off); a lock must
        // never latch a publication or source-schema block.
        let blocked = status(&config)["blocked_tables"].clone();
        assert!(
            blocked.as_array().into_iter().flatten().all(|record| {
                record["error_code"] != "publication_changed"
                    && record["error_code"] != "source_schema_incompatible"
            }),
            "a lock must not block a table: {blocked}"
        );
    })
    .await;
    drop(sql_task);
}

/// pgoutput streams a transaction's TRUNCATE, DDL Relation and rows once it
/// exceeds logical_decoding_work_mem, before PostgreSQL commits or aborts it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_rolled_back_streamed_changes_do_not_block_tables() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    // Twice the decoding budget of ~300-byte rows forces streaming.
    let budget: i64 = sql
        .query_one(
            "SELECT setting::bigint * 1024 FROM pg_catalog.pg_settings WHERE name = 'logical_decoding_work_mem'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let rows = budget * 2 / 256 + 100;
    let held_url = url.clone();
    with_daemon(&url, sql, "streamed", |config, name| async move {
        let fill = |from: i64| {
            format!(
                "INSERT INTO {name}.items SELECT g, repeat(md5(g::text), 8) FROM generate_series({from}, {}) g",
                from + rows
            )
        };
        // Taken before the probe so unrelated WAL cannot outrun it.
        let mark = current(sql).await;
        sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (2, 'before')"))
            .await
            .unwrap();
        // Capture must be streaming, not merely past the initial copy, before
        // a held TRUNCATE locks items.
        until("items publishes before the streamed transactions", || async {
            materialized(&config, "items") as i64 > mark
        })
        .await;
        let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
        let held_task = tokio::spawn(connection);
        let streamed = streamed_transactions(sql, &name).await;
        for (open, end) in [
            // TRUNCATE is streamed with the rows that follow it.
            (
                format!("BEGIN; TRUNCATE {name}.items; {}", fill(1_000_000)),
                "ROLLBACK".to_owned(),
            ),
            // So is the Relation describing an incompatible column type.
            (
                format!(
                    "BEGIN; ALTER TABLE {name}.items ALTER COLUMN status TYPE varchar(400); {}",
                    fill(1_000_000)
                ),
                "ROLLBACK".to_owned(),
            ),
            // A savepoint rollback discards its TRUNCATE; the rest commits.
            (
                format!(
                    "BEGIN; {}; SAVEPOINT truncated; TRUNCATE {name}.items; {}",
                    fill(2_000_000),
                    fill(3_000_000)
                ),
                format!(
                    "ROLLBACK TO SAVEPOINT truncated; INSERT INTO {name}.items VALUES (3, 'kept'); COMMIT"
                ),
            ),
        ] {
            held.batch_execute(&open).await.unwrap();
            // PostgreSQL skips streaming a transaction already known to have
            // aborted, so end it only after its changes reached the daemon.
            decoded(sql, &name, &config).await;
            held.batch_execute(&end).await.unwrap();
        }
        drop(held);
        held_task.await.unwrap().unwrap();
        // Taken before the probe so unrelated WAL cannot outrun it.
        let mark = current(sql).await;
        sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (4, 'after')"))
            .await
            .unwrap();
        until("items publishes after the rolled-back streams", || async {
            materialized(&config, "items") as i64 > mark
        })
        .await;
        until("confirmed_flush_lsn advances past the streams", || async {
            confirmed(sql, &name).await > mark
        })
        .await;
        assert!(blocked_tables(&config).is_empty(), "{:?}", status(&config));
        until("PostgreSQL reports the streamed transactions", || async {
            streamed_transactions(sql, &name).await >= streamed + 3
        })
        .await;
        // Seed, 2, the committed fill, 3 and 4: nothing from rolled-back work.
        let expected = usize::try_from(rows).unwrap() + 5;
        assert_eq!(published_rows(&name, "items").await, expected);

        // Committed, the same streamed TRUNCATE blocks the table.
        sql.batch_execute(&format!(
            "BEGIN; TRUNCATE {name}.items; {}; COMMIT",
            fill(4_000_000)
        ))
        .await
        .unwrap();
        until("a committed streamed TRUNCATE blocks items", || async {
            blocked_tables(&config)
                .iter()
                .any(|record| record["error_code"] == "source_schema_incompatible")
        })
        .await;
        let blocked = blocked_tables(&config);
        assert_eq!(blocked.len(), 1, "{blocked:?}");
    })
    .await;
    sql_task.abort();
}

/// ORM migrations add a column, then set its default or backfill it and set
/// NOT NULL, usually before the next catalog check. Only defaults that
/// backfill existing rows without row events block the table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_column_migrations_publish_optional_fields() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    with_daemon(&url, sql, "migration", |config, name| async move {
        // Taken before the probe so unrelated WAL cannot outrun it.
        let mark = current(sql).await;
        sql.batch_execute(&format!(
            "ALTER TABLE {name}.items ADD COLUMN note text;
             ALTER TABLE {name}.items ALTER COLUMN note SET DEFAULT 'defaulted';
             INSERT INTO {name}.items (id, status) VALUES (2, 'default');
             BEGIN;
             ALTER TABLE {name}.items ADD COLUMN flag integer;
             UPDATE {name}.items SET flag = 1;
             ALTER TABLE {name}.items ALTER COLUMN flag SET NOT NULL;
             ALTER TABLE {name}.items ALTER COLUMN flag SET DEFAULT 0;
             COMMIT;
             INSERT INTO {name}.items (id, status) VALUES (3, 'not null');"
        ))
        .await
        .unwrap();
        until("items publishes the migrated rows", || async {
            materialized(&config, "items") as i64 > mark
        })
        .await;
        // Span the five-second catalog refresh as well as commit validation.
        tokio::time::sleep(Duration::from_secs(6)).await;
        // Taken before the probe so unrelated WAL cannot outrun it.
        let mark = current(sql).await;
        sql.batch_execute(&format!(
            "INSERT INTO {name}.items (id, status) VALUES (4, 'after refresh')"
        ))
        .await
        .unwrap();
        until("items publishes after the catalog refresh", || async {
            materialized(&config, "items") as i64 > mark
        })
        .await;
        assert!(blocked_tables(&config).is_empty(), "{:?}", status(&config));
        assert_eq!(published_rows(&name, "items").await, 4);
        let catalog = crate::services::TEST_CATALOGS
            .lock()
            .unwrap()
            .get(&format!("memory://{name}"))
            .cloned()
            .unwrap();
        let table = catalog
            .load_table(&iceberg::TableIdent::new(
                iceberg::NamespaceIdent::new(name.clone()),
                "items".into(),
            ))
            .await
            .unwrap();
        let schema = table.metadata().current_schema();
        for field in ["note", "flag"] {
            // Source NOT NULL still maps to an optional field: Iceberg cannot
            // make an existing optional field required.
            assert!(!schema.field_by_name(field).unwrap().required, "{field}");
        }
    })
    .await;
    sql_task.abort();
}

/// DDL before a savepoint streams its Relation with the savepoint's rows. The
/// table blocks at the commit when those rows survive; when they are rolled
/// back, the committed DDL blocks at the next Relation or catalog refresh.
/// Either way the other table keeps publishing and no row of the new shape
/// reaches the blocked table.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_streamed_ddl_blocks_only_when_it_commits() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let sql = &sql;
    let budget: i64 = sql
        .query_one(
            "SELECT setting::bigint * 1024 FROM pg_catalog.pg_settings WHERE name = 'logical_decoding_work_mem'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let rows = budget * 2 / 256 + 100;
    for (label, ddl, column, rolled_back) in [
        ("rename_rb", "RENAME COLUMN status TO label", "label", true),
        ("rename", "RENAME COLUMN status TO label", "label", false),
        (
            "default_rb",
            "ADD COLUMN extra text DEFAULT 'x'",
            "status",
            true,
        ),
        (
            "default",
            "ADD COLUMN extra text DEFAULT 'x'",
            "status",
            false,
        ),
    ] {
        let held_url = url.clone();
        with_daemon(&url, sql, label, |config, name| async move {
            // Taken before the probe so unrelated WAL cannot outrun it.
            let mark = current(sql).await;
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (2, 'before')"))
                .await
                .unwrap();
            until("both tables publish before the DDL", || async {
                materialized(&config, "orders") as i64 > mark
                    && materialized(&config, "items") > 0
            })
            .await;
            let items = published_rows(&name, "items").await;
            let (held, connection) = tokio_postgres::connect(&held_url, NoTls).await.unwrap();
            let held_task = tokio::spawn(connection);
            held.batch_execute(&format!(
                "BEGIN; ALTER TABLE {name}.items {ddl}; SAVEPOINT s;
                 INSERT INTO {name}.items (id, {column}) SELECT g, repeat(md5(g::text), 8) FROM generate_series(1000, {}) g",
                1000 + rows
            ))
            .await
            .unwrap();
            decoded(sql, &name, &config).await;
            // Another transaction commits while the streamed one is undecided.
            // Taken before the probe so unrelated WAL cannot outrun it.
            let mark = current(sql).await;
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'during')"))
                .await
                .unwrap();
            until("orders publishes during the streamed DDL", || async {
                materialized(&config, "orders") as i64 > mark
            })
            .await;
            let end = if rolled_back { "ROLLBACK TO SAVEPOINT s; COMMIT" } else { "COMMIT" };
            held.batch_execute(end).await.unwrap();
            drop(held);
            held_task.await.unwrap().unwrap();
            // A row of the committed new shape.
            sql.batch_execute(&format!(
                "INSERT INTO {name}.items (id, {column}) VALUES (4, 'new shape')"
            ))
            .await
            .unwrap();
            until("the committed DDL blocks items", || async {
                blocked_tables(&config)
                    .iter()
                    .any(|record| record["error_code"] == "source_schema_incompatible")
            })
            .await;
            let blocked = blocked_tables(&config);
            assert_eq!(blocked.len(), 1, "{label}: {blocked:?}");
            // Taken before the probe so unrelated WAL cannot outrun it.
            let mark = current(sql).await;
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (5, 'after')"))
                .await
                .unwrap();
            until("orders keeps publishing after the block", || async {
                materialized(&config, "orders") as i64 > mark
            })
            .await;
            assert_eq!(published_rows(&name, "items").await, items, "{label}");
            assert_eq!(published_rows(&name, "orders").await, 4, "{label}");
        })
        .await;
    }
    sql_task.abort();
}

fn test_catalog(name: &str) -> Arc<dyn iceberg::Catalog> {
    crate::services::TEST_CATALOGS
        .lock()
        .unwrap()
        .get(&format!("memory://{name}"))
        .cloned()
        .unwrap()
}

/// Commit a row to `{name}.items` as another engine would: a plain Iceberg
/// append without a Flow operation ID. Returns the snapshot it replaced.
async fn append_externally(config: &Config, name: &str) -> i64 {
    use iceberg::transaction::{ApplyTransactionAction, Transaction};
    let catalog = test_catalog(name);
    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(name.into()),
            "items".into(),
        ))
        .await
        .unwrap();
    let before = table.metadata().current_snapshot_id().unwrap();
    let mut writer = flow_materializer::DataWriter::new(
        table.file_io().clone(),
        table.metadata().location(),
        &flow_model::OperationId("external-append".into()),
        config.tables[1].schema(0),
        0,
        flow_materializer::WriterConfig::default(),
    )
    .unwrap();
    writer
        .write(
            &[vec![
                flow_model::Value::Int64(900),
                flow_model::Value::String("external".into()),
            ]],
            flow_model::PgLsn(0),
        )
        .await
        .unwrap();
    let transaction = Transaction::new(&table);
    transaction
        .fast_append()
        .add_data_files(writer.close().await.unwrap())
        .apply(transaction)
        .unwrap()
        .commit(catalog.as_ref())
        .await
        .unwrap();
    before
}

/// Point `{name}.items` back at `snapshot`, as an operator undoing a foreign write.
async fn roll_back(name: &str, snapshot: i64) {
    use iceberg::spec::{SnapshotReference, SnapshotRetention};
    use iceberg::{TableRequirement, TableUpdate};
    let catalog = test_catalog(name);
    let table = catalog
        .load_table(&iceberg::TableIdent::new(
            iceberg::NamespaceIdent::new(name.into()),
            "items".into(),
        ))
        .await
        .unwrap();
    let commit = iceberg::TableCommit::builder()
        .ident(table.identifier().clone())
        .updates(vec![TableUpdate::SetSnapshotRef {
            ref_name: "main".into(),
            reference: SnapshotReference {
                snapshot_id: snapshot,
                retention: SnapshotRetention::Branch {
                    min_snapshots_to_keep: None,
                    max_snapshot_age_ms: None,
                    max_ref_age_ms: None,
                },
            },
        }])
        .requirements(vec![TableRequirement::UuidMatch {
            uuid: table.metadata().uuid(),
        }])
        .build();
    catalog.update_table(commit).await.unwrap();
}

fn external_block(config: &Config) -> Option<serde_json::Value> {
    blocked_tables(config)
        .into_iter()
        .find(|record| record["error_code"] == "external_mutation")
}

/// Another engine appends a row to the `items` target. Only `items` blocks,
/// with `external_mutation`, while `orders` keeps publishing. `rollback`
/// undoes the foreign snapshot within the grace, which must resume `items`
/// with every source change; otherwise the grace expires and `items` stops
/// holding the source acknowledgement while its target stays unchanged.
async fn external_write(url: &str, sql: &Client, rollback: bool) {
    let label = if rollback { "rollback" } else { "expiry" };
    let grace = if rollback { 600 } else { 2 };
    with_configured_daemon(
        url,
        sql,
        label,
        |config| config.limits.external_mutation_grace_secs = grace,
        |config, name| async move {
            sql.batch_execute(&format!(
                "BEGIN; INSERT INTO {name}.orders VALUES (2, 'before'); INSERT INTO {name}.items VALUES (2, 'before'); COMMIT"
            ))
            .await
            .unwrap();
            until("both tables publish before the foreign write", || async {
                materialized(&config, "orders") > 0 && materialized(&config, "items") > 0
                    && published_rows(&name, "items").await == 2
            })
            .await;
            let original = append_externally(&config, &name).await;

            sql.batch_execute(&format!("INSERT INTO {name}.items VALUES (3, 'after')"))
                .await
                .unwrap();
            let mark = current(sql).await;
            sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (3, 'after')"))
                .await
                .unwrap();
            let healthy = current(sql).await;
            until("items is blocked with external_mutation", || async {
                external_block(&config).is_some()
            })
            .await;
            assert_eq!(blocked_tables(&config).len(), 1);
            until("orders keeps publishing past the foreign write", || async {
                materialized(&config, "orders") as i64 >= healthy
            })
            .await;

            if rollback {
                let record = external_block(&config).unwrap();
                assert!(record["resync_after_ms"].as_u64().is_some(), "{record}");
                assert!(record.get("resync_required").is_none(), "{record}");
                // Within the grace the table holds its source WAL.
                assert!(confirmed(sql, &name).await < mark);
                roll_back(&name, original).await;
                until("rolling back the foreign snapshot clears the block", || async {
                    blocked_tables(&config).is_empty()
                })
                .await;
                until("items publishes the changes it held", || async {
                    materialized(&config, "items") as i64 >= mark
                        && published_rows(&name, "items").await == 3
                })
                .await;
                until("the source acknowledgement passes items again", || async {
                    confirmed(sql, &name).await >= mark
                })
                .await;
            } else {
                until("the grace passes and items requires a resync", || async {
                    external_block(&config)
                        .is_some_and(|record| record["resync_required"] == true)
                })
                .await;
                until("the source acknowledgement passes the blocked table", || async {
                    confirmed(sql, &name).await >= healthy
                })
                .await;
                // Its target keeps the foreign row and never receives row 3.
                assert_eq!(published_rows(&name, "items").await, 3);
                let rows = published_rows(&name, "orders").await;
                sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (4, 'later')"))
                    .await
                    .unwrap();
                until("orders keeps publishing after the grace", || async {
                    published_rows(&name, "orders").await == rows + 1
                })
                .await;
                assert_eq!(published_rows(&name, "items").await, 3);
                assert!(external_block(&config).is_some());
            }
        },
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_external_writes_block_one_table_until_rollback_or_grace() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    external_write(&url, &sql, true).await;
    external_write(&url, &sql, false).await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

/// A table larger than one COPY stream's minimum is read as several `ctid`
/// page ranges under one snapshot. Updated, deleted and moved row versions
/// before the cut must each be copied exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires FLOW_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn live_initial_copy_splits_large_tables_into_page_ranges() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let seed = "INSERT INTO {name}.orders
                    SELECT id, repeat('order-', 30) || id FROM generate_series(2, 20000) id;
                UPDATE {name}.orders SET status = status || '-updated' WHERE id % 3 = 0;
                DELETE FROM {name}.orders WHERE id % 7 = 0;
                INSERT INTO {name}.orders
                    SELECT id, 'late-' || id FROM generate_series(20001, 20500) id;
                UPDATE {name}.orders SET status = NULL WHERE id % 11 = 0;";
    with_seeded_daemon(
        &url,
        &sql,
        "copy_ranges",
        seed,
        |_| {},
        |_config, name| {
            let sql = &sql;
            async move {
                let pages: i64 = sql
                    .query_one(
                        &format!(
                            "SELECT pg_catalog.pg_relation_size('{name}.orders') \
                         / pg_catalog.current_setting('block_size')::bigint"
                        ),
                        &[],
                    )
                    .await
                    .unwrap()
                    .get(0);
                let ranges = flow_pg_source::PageRange::split(
                    u64::try_from(pages).unwrap(),
                    crate::bootstrap::range_pages(),
                );
                assert!(
                    ranges.len() > 1,
                    "the table must span several ranges: {pages} pages"
                );
                let expected: Vec<(i64, Option<String>)> = sql
                    .query(
                        &format!("SELECT id, status FROM {name}.orders ORDER BY id"),
                        &[],
                    )
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|row| (row.get(0), row.get(1)))
                    .collect();
                assert_eq!(expected.len(), 20_500 - 20_000 / 7);
                until("initial copy published", || async {
                    published_rows(&name, "orders").await == expected.len()
                })
                .await;
                assert_eq!(published_values(&name, "orders").await, expected);
            }
        },
    )
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

/// Run the daemon, without compaction, until `checks` finish.
async fn run_until(config: &Config, checks: impl Future<Output = ()>) {
    run_until_with(config, false, checks).await
}

async fn run_until_with(config: &Config, compaction: bool, checks: impl Future<Output = ()>) {
    {
        let daemon = crate::runtime::run(config.clone(), compaction);
        tokio::pin!(daemon);
        tokio::select! {
            result = &mut daemon => panic!("daemon stopped: {result:?}"),
            () = checks => {}
        }
    }
    // A stopped daemon's capture thread releases the state lock shortly
    // after; a process exit releases it at once.
    tokio::time::sleep(Duration::from_millis(1500)).await;
}

async fn source_values(sql: &Client, name: &str, table: &str) -> Vec<(i64, Option<String>)> {
    sql.query(
        &format!("SELECT id, status FROM {name}.{table} ORDER BY id"),
        &[],
    )
    .await
    .unwrap()
    .into_iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

/// A table added to the configuration of a running source is copied under a
/// temporary snapshot beside capture, without a resync: its target matches
/// the source exactly, including changes made while it was copied, and the
/// other table keeps publishing throughout and after restarts.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL"]
async fn live_added_table_copies_beside_capture() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let seed = "INSERT INTO {name}.items
                    SELECT id, repeat('item-', 30) || id FROM generate_series(2, 20000) id;
                DELETE FROM {name}.items WHERE id % 9 = 0;";
    with_source(&url, &sql, "add_table", seed, |_| {}, |config, name| {
        let (sql, url) = (&sql, &url);
        async move {
            let mut original = config.clone();
            original.tables.truncate(1);
            crate::bootstrap::initialize(original.clone()).await.unwrap();
            run_until(&original, async {
                sql.batch_execute(&format!("INSERT INTO {name}.orders VALUES (2, 'before')"))
                    .await
                    .unwrap();
                until("orders publish before the addition", || async {
                    published_values(&name, "orders").await == source_values(sql, &name, "orders").await
                })
                .await;
            })
            .await;

            // Add `items` while both tables keep changing.
            let (writer_sql, writer_connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
            let writer_task = tokio::spawn(writer_connection);
            let writes = {
                let name = name.clone();
                async move {
                    for step in 0..60i64 {
                        writer_sql
                            .batch_execute(&format!(
                                "BEGIN;
                                 UPDATE {name}.items SET status = 'moved-{step}' WHERE id % 60 = {step};
                                 INSERT INTO {name}.items VALUES ({id}, 'added-{step}');
                                 DELETE FROM {name}.items WHERE id = {gone};
                                 UPDATE {name}.items SET id = id + 100000 WHERE id = {rekey};
                                 INSERT INTO {name}.orders VALUES ({order}, 'during-{step}');
                                 COMMIT",
                                id = 40_000 + step,
                                gone = 1_000 + step,
                                rekey = 2_000 + step,
                                order = 10 + step,
                            ))
                            .await
                            .unwrap();
                        tokio::time::sleep(Duration::from_millis(40)).await;
                    }
                }
            };
            run_until_with(&config, true, async {
                writes.await;
                until("the added table matches its source", || async {
                    published_values(&name, "items").await == source_values(sql, &name, "items").await
                })
                .await;
                until("orders kept publishing", || async {
                    published_values(&name, "orders").await == source_values(sql, &name, "orders").await
                })
                .await;
            })
            .await;
            drop(writer_task);
            assert!(blocked_tables(&config).is_empty(), "{:?}", blocked_tables(&config));

            // The added table is an ordinary table after a restart.
            run_until_with(&config, true, async {
                sql.batch_execute(&format!(
                    "BEGIN; UPDATE {name}.items SET status = 'after-restart' WHERE id % 13 = 0;
                     DELETE FROM {name}.items WHERE id % 17 = 0;
                     INSERT INTO {name}.orders VALUES (500, 'after-restart'); COMMIT"
                ))
                .await
                .unwrap();
                until("both tables match after a restart", || async {
                    published_values(&name, "items").await == source_values(sql, &name, "items").await
                        && published_values(&name, "orders").await
                            == source_values(sql, &name, "orders").await
                })
                .await;
            })
            .await;
        }
    })
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

/// Stopping the daemon at any point of an added table's copy loses nothing:
/// the next start recovers the publication, publishes a complete staged
/// copy, or copies again from a new snapshot.
#[tokio::test]
#[ignore = "requires FLOW_POSTGRES_URL"]
async fn live_added_table_copy_survives_interruptions() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let seed = "INSERT INTO {name}.items
                    SELECT id, repeat('item-', 30) || id FROM generate_series(2, 60000) id;";
    with_source(&url, &sql, "add_interrupted", seed, |_| {}, |config, name| {
        let sql = &sql;
        async move {
            let mut original = config.clone();
            original.tables.truncate(1);
            crate::bootstrap::initialize(original.clone()).await.unwrap();
            run_until(&original, async {
                until("orders publish before the addition", || async {
                    published_values(&name, "orders").await == source_values(sql, &name, "orders").await
                })
                .await;
            })
            .await;
            for (attempt, millis) in [60u64, 150, 300, 500, 800, 1200].into_iter().enumerate() {
                sql.batch_execute(&format!(
                    "BEGIN; UPDATE {name}.items SET status = 'attempt-{attempt}' WHERE id % 7 = {attempt};
                     DELETE FROM {name}.items WHERE id % 1000 = {attempt};
                     INSERT INTO {name}.items VALUES ({id}, 'attempt-{attempt}');
                     INSERT INTO {name}.orders VALUES ({order}, 'attempt-{attempt}'); COMMIT",
                    id = 70_000 + attempt,
                    order = 100 + attempt,
                ))
                .await
                .unwrap();
                let _ = tokio::time::timeout(
                    Duration::from_millis(millis),
                    crate::runtime::run(config.clone(), true),
                )
                .await;
                // Let detached blocking work of the stopped daemon finish.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            run_until_with(&config, true, async {
                until("the interrupted addition converges", || async {
                    published_values(&name, "items").await == source_values(sql, &name, "items").await
                        && published_values(&name, "orders").await
                            == source_values(sql, &name, "orders").await
                })
                .await;
            })
            .await;
        }
    })
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

/// Measurement, not a check: copy an added table of `FLOW_ADD_TABLE_ROWS`
/// rows while another table takes a steady stream of commits, and report the
/// copy's rate and the other table's commit-to-visible lag before, during
/// and after it.
#[tokio::test]
#[ignore = "measurement; requires FLOW_POSTGRES_URL"]
async fn measure_added_table_copy_beside_capture() {
    let url = std::env::var("FLOW_POSTGRES_URL").expect("FLOW_POSTGRES_URL");
    let rows: i64 = std::env::var("FLOW_ADD_TABLE_ROWS")
        .ok()
        .and_then(|rows| rows.parse().ok())
        .unwrap_or(1_000_000);
    let (sql, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    let sql_task = tokio::spawn(connection);
    let seed = format!(
        "INSERT INTO {{name}}.items SELECT id, md5(id::text) || repeat('x', 100) FROM generate_series(2, {rows}) id;"
    );
    with_source(&url, &sql, "add_measure", &seed, |_| {}, |config, name| {
        let url = &url;
        async move {
            let mut original = config.clone();
            original.tables.truncate(1);
            crate::bootstrap::initialize(original.clone()).await.unwrap();
            run_until(&original, async {
                until("orders publish", || async { published_rows(&name, "orders").await == 1 }).await;
            })
            .await;
            // A writer commits one order every 20 ms; a prober samples how
            // far the published count trails it.
            let (writer, writer_connection) = tokio_postgres::connect(url, NoTls).await.unwrap();
            let writer_task = tokio::spawn(writer_connection);
            let written = Arc::new(std::sync::Mutex::new(Vec::<Instant>::new()));
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let writing = {
                let (written, stop, name) = (written.clone(), stop.clone(), name.clone());
                tokio::spawn(async move {
                    let mut id = 10i64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        writer
                            .execute(&format!("INSERT INTO {name}.orders VALUES ({id}, 'steady')"), &[])
                            .await
                            .unwrap();
                        written.lock().unwrap().push(Instant::now());
                        id += 1;
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
            };
            let started = Instant::now();
            let mut samples: Vec<(f64, f64, bool)> = Vec::new();
            let mut copied_at = None;
            // Compaction runs, as in production: without it, L0 debt pauses
            // the other table's publication regardless of the copy.
            run_until_with(&config, true, async {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    let visible = published_rows(&name, "orders").await.saturating_sub(1);
                    let lag = {
                        let written = written.lock().unwrap();
                        written
                            .get(visible)
                            .map_or(0.0, |oldest_unpublished| oldest_unpublished.elapsed().as_secs_f64())
                    };
                    let copying = copied_at.is_none();
                    samples.push((started.elapsed().as_secs_f64(), lag, copying));
                    // `rows` with the harness's seed row.
                    if copied_at.is_none() && published_rows(&name, "items").await as i64 == rows {
                        copied_at = Some(started.elapsed());
                    }
                    if copied_at.is_some_and(|at| started.elapsed() > at + Duration::from_secs(10)) {
                        break;
                    }
                }
            })
            .await;
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            writing.await.unwrap();
            drop(writer_task);
            let copied = copied_at.unwrap().as_secs_f64();
            let percentile = |during: bool, p: f64| {
                let mut lags: Vec<f64> = samples.iter().filter(|s| s.2 == during).map(|s| s.1).collect();
                lags.sort_by(f64::total_cmp);
                lags.get(((lags.len() as f64 - 1.0) * p) as usize).copied().unwrap_or(0.0)
            };
            println!(
                "ADD_TABLE_MEASURE rows={rows} copy_seconds={copied:.1} rows_per_second={:.0} \
                 other_table_lag_during_copy p50={:.2}s p95={:.2}s max={:.2}s; after p50={:.2}s p95={:.2}s",
                rows as f64 / copied,
                percentile(true, 0.5),
                percentile(true, 0.95),
                percentile(true, 1.0),
                percentile(false, 0.5),
                percentile(false, 0.95),
            );
        }
    })
    .await;
    drop(sql);
    sql_task.await.unwrap().unwrap();
}

//! SQLite storage. A single writer connection behind a mutex: at a handful of
//! nodes reporting every few seconds, every statement here is sub-millisecond.
// ponytail: single global connection; move to a read pool if the dashboard ever
// blocks behind ingest.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use anyhow::{Context, Result};
use chrono::{Datelike, Local, NaiveDate, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use tracing::info;

pub use crate::db_plugins::PluginRow;

pub struct Db(Mutex<Connection>);

/// 供本文件与 db_plugins 的测试共用的内存库。
#[cfg(test)]
pub(crate) fn db() -> Db {
    Db::open(":memory:").unwrap()
}

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
-- 8 MiB of page cache. The whole working set of a few hundred nodes fits, so
-- the read paths stop going back to the filesystem.
PRAGMA cache_size = -8192;
-- Without these the WAL grows to whatever the busiest minute needed and never
-- gives the space back: a hub is a long-running process on a small VPS.
PRAGMA wal_autocheckpoint = 256;
PRAGMA journal_size_limit = 1048576;

CREATE TABLE IF NOT EXISTS setting (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS node (
  id            INTEGER PRIMARY KEY,
  name          TEXT    NOT NULL,
  -- The agent's credential, in the clear: the panel shows a node's install
  -- command whenever it is asked, so it has to be able to read it back.
  token         TEXT    NOT NULL UNIQUE,
  sort          INTEGER NOT NULL DEFAULT 0,
  public        INTEGER NOT NULL DEFAULT 1,
  remark        TEXT    NOT NULL DEFAULT '',
  traffic_limit INTEGER NOT NULL DEFAULT 0,
  traffic_mode  TEXT    NOT NULL DEFAULT 'sum',
  traffic_reset_day INTEGER NOT NULL DEFAULT 1,
  hostname TEXT NOT NULL DEFAULT '', os TEXT NOT NULL DEFAULT '',
  kernel   TEXT NOT NULL DEFAULT '', arch TEXT NOT NULL DEFAULT '',
  virt     TEXT NOT NULL DEFAULT '', cpu_name TEXT NOT NULL DEFAULT '',
  cpu_cores INTEGER NOT NULL DEFAULT 0, mem_total INTEGER NOT NULL DEFAULT 0,
  swap_total INTEGER NOT NULL DEFAULT 0, disk_total INTEGER NOT NULL DEFAULT 0,
  agent_version TEXT NOT NULL DEFAULT '', ip TEXT NOT NULL DEFAULT '',
  ipv4 TEXT NOT NULL DEFAULT '', ipv6 TEXT NOT NULL DEFAULT '',
  -- The address this node's last connection was observed from, as the hub saw
  -- it. Distinct from `ip`: that one holds the geo address the country lookup
  -- keys on and may be an address the agent reported about itself, while this
  -- one is evidence of where the connection actually arrived from.
  observed_ip TEXT NOT NULL DEFAULT '',
  -- ISO 3166-1 alpha-2, looked up from `ip` once per address. Empty until the
  -- lookup answers, and empty is what a node whose country nobody could tell
  -- stays: the public page just leaves the badge off.
  country TEXT NOT NULL DEFAULT '',
  -- Survives the disconnection it describes, unlike the in-memory live entry:
  -- an offline node's page is exactly where "since when" is worth reading.
  last_seen INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL
);

-- Monotonic byte counters that survive both agent reboots and hub restarts.
CREATE TABLE IF NOT EXISTS traffic (
  node_id  INTEGER PRIMARY KEY REFERENCES node(id) ON DELETE CASCADE,
  boot_id  TEXT    NOT NULL DEFAULT '',
  last_rx  INTEGER NOT NULL DEFAULT 0,
  last_tx  INTEGER NOT NULL DEFAULT 0,
  total_rx INTEGER NOT NULL DEFAULT 0,
  total_tx INTEGER NOT NULL DEFAULT 0,
  month_rx INTEGER NOT NULL DEFAULT 0,
  month_tx INTEGER NOT NULL DEFAULT 0,
  month_start TEXT NOT NULL DEFAULT '',
  day_rx INTEGER NOT NULL DEFAULT 0,
  day_tx INTEGER NOT NULL DEFAULT 0,
  day_start TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS metric (
  node_id INTEGER NOT NULL REFERENCES node(id) ON DELETE CASCADE,
  ts      INTEGER NOT NULL,
  cpu REAL NOT NULL,
  mem_used INTEGER NOT NULL, swap_used INTEGER NOT NULL, disk_used INTEGER NOT NULL,
  net_rx INTEGER NOT NULL, net_tx INTEGER NOT NULL,
  tcp INTEGER NOT NULL, udp INTEGER NOT NULL, procs INTEGER NOT NULL,
  PRIMARY KEY (node_id, ts)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS ping_task (
  id       INTEGER PRIMARY KEY,
  name     TEXT    NOT NULL,
  target   TEXT    NOT NULL,
  interval INTEGER NOT NULL DEFAULT 60
);

CREATE TABLE IF NOT EXISTS ping_node (
  task_id INTEGER NOT NULL REFERENCES ping_task(id) ON DELETE CASCADE,
  node_id INTEGER NOT NULL REFERENCES node(id) ON DELETE CASCADE,
  PRIMARY KEY (task_id, node_id)
);

-- Key order follows the only query there is: one node, one time window,
-- every probe. With task_id ahead of ts SQLite can seek to the node and no
-- further, then scans every record it ever kept -- see the migration in open().
CREATE TABLE IF NOT EXISTS ping_record (
  node_id INTEGER NOT NULL, task_id INTEGER NOT NULL,
  ts INTEGER NOT NULL, latency INTEGER NOT NULL,
  PRIMARY KEY (node_id, ts, task_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS session (
  token_hash TEXT    PRIMARY KEY,
  expires_at INTEGER NOT NULL
);

-- Uploaded notification plugins: the manifest and the wasm module itself, so a
-- restart needs nothing from the filesystem beyond the database.
CREATE TABLE IF NOT EXISTS plugin (
  id INTEGER PRIMARY KEY,
  plugin_id TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL DEFAULT '',
  version TEXT NOT NULL DEFAULT '',
  manifest_json TEXT NOT NULL DEFAULT '',
  wasm_blob BLOB NOT NULL,
  wasm_sha256 TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'disabled',
  last_error TEXT,
  uploaded_at INTEGER NOT NULL
);

-- One row per dispatch the hub has made. The key is the idempotency key: the
-- same node, event and billing cycle must never notify twice, while a state
-- event (agent_offline/agent_online) holds one mutable row per node per side.
CREATE TABLE IF NOT EXISTS notification_log (
  node_id INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  threshold_or_state_key INTEGER NOT NULL,
  sent_at INTEGER NOT NULL,
  success INTEGER NOT NULL DEFAULT 0,
  detail TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (node_id, event_type, threshold_or_state_key)
);

-- 通用插件数据存储(U2/KTD3):插件对自己命名空间的记录集有完整 CRUD。
-- 不复用 setting 的 KV(值上限 8 KiB、无结构):插件数据是记录集,`data`
-- 存 JSON。物理隔离在 (plugin_id, record_key) 主键上——宿主函数按调用方
-- plugin_id 寻址,一个插件够不到另一个的行(R2)。
CREATE TABLE IF NOT EXISTS plugin_data (
  plugin_id  TEXT    NOT NULL,
  record_key TEXT    NOT NULL,
  data       TEXT    NOT NULL DEFAULT '',
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (plugin_id, record_key)
);
"#;

/// Schema revision this build expects, stamped into `PRAGMA user_version`.
/// Increment it and add a `migrate_to_N` when the schema changes under a
/// database already in service.
const SCHEMA_VERSION: i64 = 7;

/// 两段式删列迁移(GATED_VERSION)未完成时停留的版本号。写成一个**绝对**的
/// 常量而不是 `SCHEMA_VERSION - 1`:后者会随下一次升版一起漂走,把「v6 迁移
/// 没跑过」的库盖成一个它其实没到过的版本。
const GATED_VERSION: i64 = 5;

/// node 表退役的四列(U7)。删列迁移的清单与完成判据都引用它,不散写。
const RETIRED_NODE_COLUMNS: [&str; 4] = ["price", "currency", "billing_cycle", "expires_at"];

/// 财务插件的 plugin_id。删列闸门只认它写下的 `node:` 记录——闸门读的是该
/// 插件的私有 key 布局,不能因为别的插件恰好用了同一前缀就打开。
///
/// 跨仓不变量:这个值同时是财务插件在 CarlJia/monitor-hub-plugins 里
/// `finance-stats/plugin.toml` 的 `plugin_id`。任一侧改动都会让下面的 v6
/// 删列门控永远完成不了(imported==0 → 保留旧列下次重试),且两仓的 cargo
/// 测试都不会红。ci.yml 的 `plugin-abi` job 断言两侧一致,改此值前先同步插件仓。
const FINANCE_PLUGIN_ID: &str = "io.github.monitor.finance-stats";

/// Adds a column older databases lack. A duplicate column indicates the
/// migration has already run; every other error must propagate.
fn add_column(conn: &Connection, table: &str, column: &str) -> Result<()> {
    match conn.execute(&format!("ALTER TABLE {table} ADD COLUMN {column}"), []) {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column name") => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// True when `table`'s stored DDL contains `needle`, which is how a migration
/// determines the shape of the database it inherited.
fn schema_mentions(conn: &Connection, table: &str, needle: &str) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE name=?1 AND sql LIKE ?2",
        params![table, format!("%{needle}%")],
        |r| r.get::<_, i64>(0),
    )? > 0)
}

/// One table's column names. `table` is always a [`TABLES`] entry rather than
/// caller-supplied, which is why it can be formatted into the pragma.
fn columns_of(conn: &Connection, table: &str) -> Result<HashSet<String>> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let names = stmt.query_map([], |r| r.get::<_, String>(1))?;
    Ok(names.collect::<Result<_, _>>()?)
}

/// Everything accumulated before a version was recorded. Runs once, on a
/// database predating the stamp.
fn migrate_to_1(conn: &Connection) -> Result<()> {
    for column in [
        "day_rx INTEGER NOT NULL DEFAULT 0",
        "day_tx INTEGER NOT NULL DEFAULT 0",
        "day_start TEXT NOT NULL DEFAULT ''",
    ] {
        add_column(conn, "traffic", column)?;
    }
    for column in [
        "ipv4 TEXT NOT NULL DEFAULT ''",
        "ipv6 TEXT NOT NULL DEFAULT ''",
        "last_seen INTEGER NOT NULL DEFAULT 0",
    ] {
        add_column(conn, "node", column)?;
    }
    // The column held a sha256 of the token and now holds the token itself.
    // Databases predating the change retain digests no agent can present, so
    // those nodes require a new token issued from the panel.
    if schema_mentions(conn, "node", "token_hash")? {
        conn.execute("ALTER TABLE node RENAME COLUMN token_hash TO token", [])?;
        info!("renamed node.token_hash to node.token; existing nodes need a fresh token");
    }
    // Reordering a key requires rebuilding the table; CREATE TABLE IF NOT EXISTS
    // leaves an existing one untouched. The old order placed task_id between the
    // node and the timestamp, so the chart query scanned a node's entire history
    // to answer for one hour of it: 42 ms against 0.8 ms at a month of
    // retention.
    if schema_mentions(conn, "ping_record", "(node_id, task_id, ts)")? {
        conn.execute_batch(
            "BEGIN;
             CREATE TABLE ping_record_rekeyed (
               node_id INTEGER NOT NULL, task_id INTEGER NOT NULL,
               ts INTEGER NOT NULL, latency INTEGER NOT NULL,
               PRIMARY KEY (node_id, ts, task_id)
             ) WITHOUT ROWID;
             INSERT INTO ping_record_rekeyed SELECT * FROM ping_record;
             DROP TABLE ping_record;
             ALTER TABLE ping_record_rekeyed RENAME TO ping_record;
             COMMIT;",
        )?;
        info!("rebuilt ping_record on a key the latency chart can seek");
    }
    Ok(())
}

/// `metric.load1` was written on every history row and read by nothing: the card
/// draws the live `load` array from the report, and no chart draws load from
/// history. Dropping it recovers 21% of what the five unread columns cost, and
/// it is the only one the hub can lose without also losing a figure the UI
/// displays.
///
/// The column is `NOT NULL` with no default, so this migration is mandatory:
/// without it every metric insert this build makes violates the constraint.
fn migrate_to_2(conn: &Connection) -> Result<()> {
    if schema_mentions(conn, "metric", "load1")? {
        conn.execute("ALTER TABLE metric DROP COLUMN load1", [])?;
        info!("dropped metric.load1; nothing read it");
    }
    Ok(())
}

fn migrate_to_3(conn: &Connection) -> Result<()> {
    add_column(conn, "node", "country TEXT NOT NULL DEFAULT ''")
}

/// v7 keeps the address a node's connection was observed from, so the panel can
/// show a NAT'd node's reachable address without depending on what that node
/// reported about itself.
///
/// The column belongs in `SCHEMA` as well, and the two halves are load-bearing
/// in opposite directions. A fresh database is built from `SCHEMA` alone --
/// `Db::open` passes `from = SCHEMA_VERSION`, so no migration runs -- and would
/// therefore lack a column only the migration knows, failing every `save_facts`
/// on the UPDATE. An old database reaches the column only through this
/// migration, and `check_backup`'s reference is built from `SCHEMA`, so a
/// column `SCHEMA` declares but this migration omits leaves a migrated backup
/// short of it and the restore is refused as missing.
///
/// Runs regardless of the v6 column-drop gate, which can stay unfinished
/// indefinitely: a database parked at `GATED_VERSION` would otherwise never get
/// this column, and every report would fail on the UPDATE instead. The stamp
/// stays `GATED_VERSION` there, so this runs again on each open and relies on
/// `add_column` tolerating the duplicate.
fn migrate_to_7(conn: &Connection) -> Result<()> {
    add_column(conn, "node", "observed_ip TEXT NOT NULL DEFAULT ''")
}

/// v4 adds the `plugin` and `notification_log` tables and moves nothing. On a
/// database opened through `Db::open` the schema batch has already created
/// them by the time any migration runs; a backup candidate in `check_backup`
/// is migrated on a bare connection, so the tables are created here rather
/// than assumed. The DDL must match SCHEMA's -- a drift is caught loudly by
/// `check_backup`, which compares a migrated backup's columns against a
/// database SCHEMA built.
fn migrate_to_4(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS plugin (
           id INTEGER PRIMARY KEY,
           plugin_id TEXT NOT NULL UNIQUE,
           name TEXT NOT NULL DEFAULT '',
           version TEXT NOT NULL DEFAULT '',
           manifest_json TEXT NOT NULL DEFAULT '',
           wasm_blob BLOB NOT NULL,
           wasm_sha256 TEXT NOT NULL DEFAULT '',
           enabled INTEGER NOT NULL DEFAULT 0,
           status TEXT NOT NULL DEFAULT 'disabled',
           last_error TEXT,
           uploaded_at INTEGER NOT NULL
         );
         CREATE TABLE IF NOT EXISTS notification_log (
           node_id INTEGER NOT NULL,
           event_type TEXT NOT NULL,
           threshold_or_state_key INTEGER NOT NULL,
           sent_at INTEGER NOT NULL,
           success INTEGER NOT NULL DEFAULT 0,
           detail TEXT NOT NULL DEFAULT '',
           PRIMARY KEY (node_id, event_type, threshold_or_state_key)
         );",
    )?;
    Ok(())
}

/// v5(U2/KTD3)新增 `plugin_data` 表:通用插件数据存储。与 v4 同一模式——
/// `Db::open` 的 SCHEMA 批已建好,备份候选走裸连接迁移故这里显式建。DDL 必须
/// 与 SCHEMA 的一致,漂移由 `check_backup` 的列比对捕获。
fn migrate_to_5(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS plugin_data (
           plugin_id  TEXT    NOT NULL,
           record_key TEXT    NOT NULL,
           data       TEXT    NOT NULL DEFAULT '',
           updated_at INTEGER NOT NULL,
           PRIMARY KEY (plugin_id, record_key)
         );",
    )?;
    Ok(())
}

/// v6(U7/KTD10)删掉 node 表退役的财务四列。这四列自 ABI v2 起没有任何代码
/// 读写——财务数据归财务插件的 `plugin_data`,唯一真源换了地方。
///
/// 删除是不可逆的,所以按 KTD10 两段式执行:只有当财务插件已经把节点导进
/// 自己的存储之后才动手。导入未完成时保留列并返回 `false`,让 `migrate` 把
/// 版本停在 [`GATED_VERSION`],下一次启动再试——「先换二进制、暂不装插件」
/// 的部署因此只是列闲置,启用插件后的下一次启动自动完成删除。
///
/// 返回是否真的完成了删列(据此决定能否推进到 v6)。
fn migrate_to_6(conn: &Connection) -> Result<bool> {
    // 一个清单同时当「完成判据」与「待删清单」:两处各写一份会漂移——此前
    // 只看 `price` 的判据,在「price 删掉了、其余还在」的半删状态下会误判成
    // 已完成,剩下三列就永远留着。
    let present = columns_of(conn, "node")?;
    let retired: Vec<&str> = RETIRED_NODE_COLUMNS.iter().copied().filter(|c| present.contains(*c)).collect();
    if retired.is_empty() {
        return Ok(true); // 已经删过,或本就是一个不含这些列的新库。
    }
    // 闸门按 plugin_id 限定:它读的是某个插件的私有 key 布局,别的插件恰好
    // 用了 `node:` 前缀不该有资格触发这次不可逆操作。
    let imported: i64 = conn.query_row(
        "SELECT COUNT(*) FROM plugin_data WHERE plugin_id = ?1 AND record_key LIKE 'node:%'",
        [FINANCE_PLUGIN_ID],
        |r| r.get(0),
    )?;
    if imported == 0 {
        return Ok(false); // 财务插件还没导入,保留列,下次启动再试。
    }
    // 旧值不会被迁移(插件读不到这四列),删除即永久丢失。删之前记一行,让
    // 运维在日志里看到这次动了多少行非默认值,而不是无声无息。
    let carried: i64 = conn.query_row(
        "SELECT COUNT(*) FROM node
          WHERE price <> 0 OR currency <> 'USD' OR billing_cycle <> 'monthly' OR expires_at IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    if carried > 0 {
        info!("schema v6: 删除 node 表退役财务列,{carried} 行带非默认值——不会被迁移,需在财务统计页面重录");
    }
    // 四条 ALTER 收在一个事务里:中途失败(磁盘满、SQLITE_BUSY)若留下半删
    // 状态,下一次会被上面那个判据误当成已完成。SQLite 的 schema 变更可事务,
    // 与 migrate_to_1 的整表重建同一手法。
    conn.execute_batch("BEGIN")?;
    for column in retired {
        conn.execute_batch(&format!("ALTER TABLE node DROP COLUMN {column}"))?;
    }
    conn.execute_batch("COMMIT")?;
    Ok(true)
}

/// Brings a database already in service up to `SCHEMA_VERSION` and stamps it.
/// `from` is its current version, so a fresh file passes `SCHEMA_VERSION` and
/// receives only the stamp.
///
/// Restoring a backup also arrives here: the copy carries its own version and
/// requires the same migrations a restart would have run.
fn migrate(conn: &Connection, from: i64) -> Result<()> {
    if from < 1 {
        migrate_to_1(conn)?;
    }
    if from < 2 {
        migrate_to_2(conn)?;
    }
    if from < 3 {
        migrate_to_3(conn)?;
    }
    if from < 4 {
        migrate_to_4(conn)?;
    }
    if from < 5 {
        migrate_to_5(conn)?;
    }
    // v6 是条件迁移:财务列要等财务插件导入完才删。未完成时停在 GATED_VERSION
    // ——若照样 stamp 成 6,这一步就永远不会重跑,列再也删不掉。
    let at_six = if from < 6 { migrate_to_6(conn)? } else { true };
    // v7 不挂在这个闸门上:它加的是别处都要用到的列,而 v6 可能永远完成不了,
    // 停在 GATED_VERSION 的库若拿不到这一列,每次报告都会失败在 UPDATE 上。
    // 于是它会随下次启动重跑,由 add_column 对重复列的容忍兜住。
    if from < 7 {
        migrate_to_7(conn)?;
    }
    let stamped = if at_six { SCHEMA_VERSION } else { GATED_VERSION };
    conn.execute_batch(&format!("PRAGMA user_version = {stamped}"))?;
    Ok(())
}

/// Every table a backup must carry before this build will restore it.
const TABLES: [&str; 11] = [
    "setting",
    "node",
    "traffic",
    "metric",
    "ping_task",
    "ping_node",
    "ping_record",
    "session",
    "plugin",
    "notification_log",
    "plugin_data",
];

/// One node's stored configuration and last known facts.
///
/// v2 起财务字段(`price`/`currency`/`billing_cycle`/`expires_at`)从宿主
/// 退役,迁入财务插件的 `plugin_data` 命名空间。旧库里的这四列由
/// `migrate_to_6` 在财务插件导入完成后删除;插件读不到旧值,只按 node_id
/// 建自己的空白记录,价格等需在插件页面重新录入。
#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Node {
    #[serde(default)]
    pub id: i64,
    pub name: String,
    #[serde(default = "yes")]
    pub public: bool,
    #[serde(default)]
    pub sort: i64,
    #[serde(default)]
    pub remark: String,
    /// Monthly allowance in bytes; 0 means unmetered.
    #[serde(default)]
    pub traffic_limit: i64,
    /// How the allowance is counted: sum, max, up or down.
    #[serde(default = "sum")]
    pub traffic_mode: String,
    #[serde(default = "one")]
    pub traffic_reset_day: u32,
    #[serde(default)]
    pub hostname: String,
    #[serde(default)]
    pub os: String,
    #[serde(default)]
    pub kernel: String,
    #[serde(default)]
    pub arch: String,
    #[serde(default)]
    pub virt: String,
    #[serde(default)]
    pub cpu_name: String,
    #[serde(default)]
    pub cpu_cores: i64,
    #[serde(default)]
    pub mem_total: i64,
    #[serde(default)]
    pub swap_total: i64,
    #[serde(default)]
    pub disk_total: i64,
    #[serde(default)]
    pub agent_version: String,
    #[serde(default)]
    pub ip: String,
    /// Reported by the agent from its own interfaces.
    #[serde(default)]
    pub ipv4: String,
    #[serde(default)]
    pub ipv6: String,
    /// Where this node's last connection arrived from, as the hub observed it,
    /// or empty for a node that has not connected since this column existed.
    /// The panel prefers an address the agent reported about itself and falls
    /// back to this one; `ip` is not an input to that choice.
    #[serde(default)]
    pub observed_ip: String,
    /// ISO 3166-1 alpha-2 for `ip`, uppercase, or empty when unknown. Public: it
    /// appears on the status page beside the node's name.
    #[serde(default)]
    pub country: String,
    /// Unix seconds of the node's last report, written once a minute alongside
    /// the metric row. Zero for a node that has never reported.
    #[serde(default)]
    pub last_seen: i64,
    /// What the agent authenticates with. Readable so the panel can display an
    /// install command on demand; it never leaves the admin view.
    #[serde(default)]
    pub token: String,
}

fn yes() -> bool {
    true
}

/// Omitted settings stay unchanged。
#[derive(Deserialize, Default)]
pub struct NodePatch {
    pub name: Option<String>,
    pub sort: Option<i64>,
    pub public: Option<bool>,
    pub remark: Option<String>,
    pub traffic_limit: Option<i64>,
    pub traffic_mode: Option<String>,
    pub traffic_reset_day: Option<u32>,
}

#[derive(Deserialize, Default)]
pub struct TrafficPatch {
    pub total_rx: Option<i64>,
    pub total_tx: Option<i64>,
    pub month_rx: Option<i64>,
    pub month_tx: Option<i64>,
}
fn sum() -> String {
    "sum".into()
}
fn one() -> u32 {
    1
}

#[derive(Serialize, Debug, Clone, Default)]
pub struct Traffic {
    pub total_rx: i64,
    pub total_tx: i64,
    pub month_rx: i64,
    pub month_tx: i64,
    pub month_start: String,
    pub day_rx: i64,
    pub day_tx: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct PingTask {
    #[serde(default)]
    pub id: i64,
    pub name: String,
    pub target: String,
    #[serde(default)]
    pub interval: i64,
    #[serde(default)]
    pub nodes: Vec<i64>,
}

/// Restricts the database to its owner.
///
/// It is the credential store: node tokens in the clear, the GitHub client
/// secret, the password hash. SQLite creates it under the umask, which at a
/// default 022 is world-readable, and the WAL and shm files hold the same rows.
///
/// Best effort: a filesystem without Unix modes still works.
fn restrict(path: &str) {
    for file in [path.to_owned(), format!("{path}-wal"), format!("{path}-shm")] {
        own_only(&file);
    }
}

/// One file, owner-only. Also applied to the backup copy `VACUUM INTO` writes,
/// which is the entire credential store in one portable file, created under the
/// umask like any other.
fn own_only(file: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o600));
    }
}

/// The `main` database's path as SQLite reports it, empty for `:memory:`.
/// Queried rather than cached so there is a single answer to which file is
/// open.
fn main_file(conn: &Connection) -> String {
    conn.query_row("PRAGMA database_list", [], |r| r.get(2)).unwrap_or_default()
}

fn bytes_of(file: &str) -> i64 {
    std::fs::metadata(file).map(|m| m.len() as i64).unwrap_or(0)
}

/// Bytes the database occupies. The WAL is included: committed rows remain there
/// until a checkpoint folds them into the main file, so the two together are what
/// an operator sees on disk.
fn on_disk(file: &str) -> i64 {
    bytes_of(file) + bytes_of(&format!("{file}-wal"))
}

/// The rows behind the latency chart: one node's probe results over a window,
/// bucketed and in time order. Everything the chart draws is folded out of them
/// in [`close_bucket`].
///
/// The key is `(node_id, ts, task_id)`, so this is a seek and the rows emerge
/// sorted without a sorter, which is what allows the fold to hold one bucket at
/// a time. Asking SQLite for the summary instead cost three sorts of the whole
/// window -- two window passes and a GROUP BY -- against this single scan: on a
/// week of four probes, 284 ms against 54 ms, all of it holding the connection
/// the agents write through.
///
/// A constant because the query plan is asserted against it in
/// `rekeying_ping_record_keeps_the_rows_and_lets_the_chart_query_seek`.
const PING_ROWS: &str = "SELECT ts/?3, task_id, latency FROM ping_record
     WHERE node_id=?1 AND ts>=?2
           AND task_id IN (SELECT task_id FROM ping_node WHERE node_id=?1)
     ORDER BY ts";

/// The whole-fleet counterpart of [`PING_ROWS`]: same rows for every node in
/// one scan, ordered by `(node_id, ts)` so the fold can hold one node's bucket
/// at a time. The batch quality endpoint needs every node's series, and N
/// per-node scans would each hold the write connection the agents report
/// through; one ordered scan pays that cost once.
///
/// The `ping_node` subquery stays correlated on the outer row's `node_id`, so a
/// probe assigned to node A contributes nothing to node B — the same
/// assignment filter the per-node query applies, expressed for all nodes.
const PING_ROWS_ALL: &str = "SELECT node_id, ts/?2, task_id, latency FROM ping_record
     WHERE ts>=?1
           AND task_id IN (SELECT task_id FROM ping_node WHERE node_id = ping_record.node_id)
     ORDER BY node_id, ts";

impl Db {
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path)?;
        // Queried before CREATE TABLE runs: a file with no tables receives the
        // current schema directly rather than the history of how it was reached.
        let fresh = conn
            .query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table'", [], |r| r.get::<_, i64>(0))?
            == 0;
        // 比本二进制新的库在动手之前就拒掉:老代码不认识新 schema,继续跑会在
        // 未知的表结构上读写,还会把 user_version 盖回自己认识的数字。与
        // `check_backup` 对上传文件的那道守卫同一句话、同一个理由。
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if !fresh && version > SCHEMA_VERSION {
            anyhow::bail!(
                "the database is from a newer hub (schema {version}, this one reads {SCHEMA_VERSION}); upgrade first"
            );
        }
        conn.execute_batch(SCHEMA)?;
        restrict(path);

        migrate(&conn, if fresh { SCHEMA_VERSION } else { version })?;
        Ok(Self(Mutex::new(conn)))
    }

    pub(crate) fn conn(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    // ---- settings ----

    pub fn get(&self, key: &str) -> Option<String> {
        self.conn()
            .query_row("SELECT value FROM setting WHERE key = ?1", [key], |r| r.get(0))
            .optional()
            .ok()
            .flatten()
    }

    /// [`Db::get`] 的传错版本。`get` 把读库失败吞成「没有这一行」,调用方因此分不出
    /// 「行不存在」与「库坏了」——对「未设就是默认」的读法这没问题,但要把这个区别
    /// 报成 400/403 还是 500 的地方(必填配置预检、host_kv_get、登录)必须用这个,
    /// 否则库故障会被说成一句自信而错误的状态。
    pub fn try_get(&self, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn()
            .query_row("SELECT value FROM setting WHERE key = ?1", [key], |r| r.get(0))
            .optional()?)
    }

    pub fn set(&self, key: &str, value: &str) -> Result<()> {
        self.conn().execute(
            "INSERT INTO setting (key, value) VALUES (?1, ?2)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![key, value],
        )?;
        Ok(())
    }

    // ---- nodes ----

    pub fn nodes(&self) -> Result<Vec<Node>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT * FROM node ORDER BY sort, id")?;
        let rows = stmt.query_map([], |r| Ok(row_to_node(r)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn node(&self, id: i64) -> Result<Option<Node>> {
        Ok(self
            .conn()
            .query_row("SELECT * FROM node WHERE id = ?1", [id], |r| Ok(row_to_node(r)))
            .optional()?)
    }

    /// `(id, name, created_at)` for every node — the projection the
    /// `nodes_query` host function sends to WASM guests. Deliberately not
    /// [`Node`]: a guest gets identity and display name only, never the token it
    /// authenticates with or the rest of a node's configuration.
    pub fn node_basics(&self) -> Result<Vec<(i64, String, i64)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT id, name, created_at FROM node ORDER BY sort, id")?;
        let rows = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// A node's `(name, created_at)` — what the notification bus needs to
    /// announce a lifecycle change. One row, one query.
    ///
    /// `created_at` is half of a node's identity: it survives SQLite handing a
    /// deleted node's id to the next node created, so a subscriber can tell
    /// "this machine is gone" from "this id now belongs to another machine".
    pub fn node_identity(&self, id: i64) -> Result<Option<(String, i64)>> {
        Ok(self
            .conn()
            .query_row("SELECT name, created_at FROM node WHERE id = ?1", [id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?)
    }

    /// Creates a node and returns its id.
    ///
    /// Both rows or neither: `accumulate` reads the `traffic` row on every
    /// report, so a node lacking one cannot report.
    pub fn create_node(&self, n: &Node, token: &str) -> Result<i64> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        tx.execute(
            // A new node belongs at the end. The caller sends sort 0, which would
            // tie with whatever the last reorder placed first.
            "INSERT INTO node (name, token, sort, public, remark, traffic_limit,
                               traffic_mode, traffic_reset_day, created_at)
             VALUES (?1,?2,(SELECT COALESCE(MAX(sort),-1)+1 FROM node),?3,?4,?5,?6,?7,?8)",
            params![
                n.name,
                token,
                n.public,
                n.remark,
                n.traffic_limit,
                n.traffic_mode,
                n.traffic_reset_day,
                Utc::now().timestamp()
            ],
        )?;
        let id = tx.last_insert_rowid();
        tx.execute("INSERT INTO traffic (node_id) VALUES (?1)", [id])?;
        tx.commit()?;
        Ok(id)
    }

    /// How many nodes were created at or after `ts`. Bounds what one registration
    /// window can add; see `api::REGISTER_LIMIT`.
    pub fn nodes_created_since(&self, ts: i64) -> Result<i64> {
        Ok(self.conn().query_row("SELECT COUNT(*) FROM node WHERE created_at >= ?1", [ts], |r| r.get(0))?)
    }

    /// Records that the node reported. Written on the same cadence as the metric
    /// row, so it costs one update per minute rather than one per report.
    pub fn touch_seen(&self, id: i64, ts: i64) -> Result<()> {
        self.conn().execute("UPDATE node SET last_seen=?2 WHERE id=?1", params![id, ts])?;
        Ok(())
    }

    pub fn update_node(&self, id: i64, n: &NodePatch) -> Result<()> {
        self.conn().execute(
            "UPDATE node SET name=COALESCE(?2,name), sort=COALESCE(?3,sort), public=COALESCE(?4,public),
                             remark=COALESCE(?5,remark), traffic_limit=COALESCE(?6,traffic_limit),
                             traffic_mode=COALESCE(?7,traffic_mode),
                             traffic_reset_day=COALESCE(?8,traffic_reset_day)
             WHERE id=?1",
            params![
                id,
                n.name,
                n.sort,
                n.public,
                n.remark,
                n.traffic_limit,
                n.traffic_mode,
                n.traffic_reset_day
            ],
        )?;
        Ok(())
    }

    pub fn reorder_nodes(&self, ids: &[i64]) -> Result<()> {
        let unique: HashSet<_> = ids.iter().collect();
        if unique.len() != ids.len() {
            anyhow::bail!("node order contains duplicates");
        }
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM node", [], |r| r.get(0))?;
        if count as usize != ids.len() {
            anyhow::bail!("node order must include every node");
        }
        for (sort, id) in ids.iter().enumerate() {
            if tx.execute("UPDATE node SET sort=?2 WHERE id=?1", params![id, sort as i64])? != 1 {
                anyhow::bail!("node order contains an unknown node");
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn delete_node(&self, id: i64) -> Result<()> {
        let conn = self.conn();
        // `ping_record` and `notification_log` carry no foreign key to `node` --
        // the former is WITHOUT ROWID and keyed for the chart query, the latter
        // is keyed by its own idempotency scheme -- so both are cleared
        // explicitly. SQLite reassigns a deleted node's id to the next node
        // created, which would otherwise inherit the removed machine's latency
        // chart and notification history: a leftover state row reading
        // "already offline" would suppress the new machine's first offline
        // alert, and leftover dispatch rows its expiry reminders.
        conn.execute("DELETE FROM ping_record WHERE node_id = ?1", [id])?;
        conn.execute("DELETE FROM notification_log WHERE node_id = ?1", [id])?;
        conn.execute("DELETE FROM node WHERE id = ?1", [id])?;
        Ok(())
    }

    /// Replaces a node's token, which immediately locks out the old one.
    pub fn reset_token(&self, id: i64, token: &str) -> Result<()> {
        self.conn().execute("UPDATE node SET token=?2 WHERE id=?1", params![id, token])?;
        Ok(())
    }

    pub fn node_by_token(&self, token: &str) -> Result<Option<i64>> {
        Ok(self.conn().query_row("SELECT id FROM node WHERE token = ?1", [token], |r| r.get(0)).optional()?)
    }

    /// Stores the slow-changing facts an agent sends on connect, and reports
    /// whether the node still requires a country lookup.
    ///
    /// A new address invalidates the previous country, so the two move together in
    /// one statement: `SET` reads the row as it was, so the comparison is against
    /// the stored address rather than the one being written.
    pub fn save_facts(&self, id: i64, f: &serde_json::Value, ip: &str, observed_ip: &str) -> Result<bool> {
        // The same rule `api::agent_register` applies to the name it receives:
        // these values come from an unvouched machine, control characters break
        // the panel's rows, and the length must be bounded. Six of them -- os,
        // kernel, arch, virt, cpu_name, agent_version -- go straight into the
        // anonymous public frame, which is rebuilt and pushed to every viewer
        // every two seconds, so without a ceiling one node would determine that
        // frame's size. 128 rather than 64: a real PRETTY_NAME runs to about 60
        // characters and a CPU model to about 50.
        let s = |k: &str| {
            f.get(k)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .chars()
                .filter(|c| !c.is_control())
                .take(128)
                .collect::<String>()
        };
        let n = |k: &str| f.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
        let conn = self.conn();
        conn.execute(
            "UPDATE node SET hostname=?2, os=?3, kernel=?4, arch=?5, virt=?6, cpu_name=?7,
                             cpu_cores=?8, mem_total=?9, swap_total=?10, disk_total=?11,
                             agent_version=?12, ip=?13, ipv4=?14, ipv6=?15, observed_ip=?16,
                             country=CASE WHEN ip=?13 THEN country ELSE '' END
             WHERE id=?1",
            params![
                id,
                s("hostname"),
                s("os"),
                s("kernel"),
                s("arch"),
                s("virt"),
                s("cpu_name"),
                n("cpu_cores"),
                n("mem_total"),
                n("swap_total"),
                n("disk_total"),
                s("agent_version"),
                ip,
                s("ipv4"),
                s("ipv6"),
                observed_ip
            ],
        )?;
        Ok(conn.query_row("SELECT country = '' FROM node WHERE id=?1", [id], |r| r.get(0))?)
    }

    /// Records the country a lookup returned, unless the node moved to another
    /// address while the lookup was outstanding. This is the same rule
    /// `save_facts` encodes in its `CASE`: the country belongs to the address it
    /// was asked about, so a late answer for an address the node has left is not
    /// an answer about the node. Kept apart from the panel's own writes:
    /// `update_node` never touches this column.
    pub fn set_country(&self, id: i64, cc: &str, ip: &str) -> Result<()> {
        self.conn().execute("UPDATE node SET country=?2 WHERE id=?1 AND ip=?3", params![id, cc, ip])?;
        Ok(())
    }

    // ---- traffic ----

    /// Every node's counters in one query, because the node list renders a row per
    /// node and a query per node would queue the agents' writes behind it.
    ///
    /// The period counters are gated on the period they were written for. They
    /// restart lazily in `accumulate`, on the node's next report, so a node
    /// offline since before a boundary still holds the previous period's bytes on
    /// disk. This is the only reader, so the rule lives in one place.
    pub fn all_traffic(&self) -> HashMap<i64, Traffic> {
        let conn = self.conn();
        let Ok(mut stmt) = conn.prepare_cached(
            "SELECT t.node_id, t.total_rx, t.total_tx, t.month_rx, t.month_tx, t.month_start,
                    t.day_rx, t.day_tx, t.day_start, n.traffic_reset_day
                 FROM traffic t JOIN node n ON n.id = t.node_id",
        ) else {
            return HashMap::new();
        };
        let today = Local::now().date_naive();
        let day = today.to_string();
        let rows = stmt.query_map([], |r| {
            // Zero rather than absent: a theme drawing a meter requires a
            // number.
            let current = |stored: String, now: &str, rx: i64, tx: i64| {
                if stored == now {
                    (rx, tx)
                } else {
                    (0, 0)
                }
            };
            let period = period_start(today, r.get(9)?).to_string();
            let (month_rx, month_tx) = current(r.get(5)?, &period, r.get(3)?, r.get(4)?);
            let (day_rx, day_tx) = current(r.get(8)?, &day, r.get(6)?, r.get(7)?);
            Ok((
                r.get::<_, i64>(0)?,
                Traffic {
                    total_rx: r.get(1)?,
                    total_tx: r.get(2)?,
                    month_rx,
                    month_tx,
                    month_start: period,
                    day_rx,
                    day_tx,
                },
            ))
        });
        rows.map(|r| r.flatten().collect()).unwrap_or_default()
    }

    /// Folds one report's raw kernel counters into the node's running totals.
    ///
    /// A changed boot_id, or a counter that moved backwards, means the kernel
    /// restarted its counting; the total must not follow it downward. `None`
    /// denotes a report carrying no readable counters at all -- see below.
    ///
    /// The billing reset day is read here rather than passed in: it is one join
    /// from a row this already reads, and fetching it separately would cost every
    /// report a second acquisition of the single write connection.
    pub fn accumulate(&self, node_id: i64, boot_id: &str, counters: Option<(i64, i64)>) -> Result<Traffic> {
        let conn = self.conn();
        let (
            prev_boot,
            last_rx,
            last_tx,
            mut total_rx,
            mut total_tx,
            mut month_rx,
            mut month_tx,
            month_start,
            mut day_rx,
            mut day_tx,
            day_start,
            reset_day,
        ) = conn
            .prepare_cached(
                "SELECT t.boot_id, t.last_rx, t.last_tx, t.total_rx, t.total_tx, t.month_rx, t.month_tx,
                    t.month_start, t.day_rx, t.day_tx, t.day_start, n.traffic_reset_day
                 FROM traffic t JOIN node n ON n.id = t.node_id WHERE t.node_id=?1",
            )?
            .query_row([node_id], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, i64>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, i64>(9)?,
                    r.get::<_, String>(10)?,
                    r.get::<_, u32>(11)?,
                ))
            })?;

        // Only bytes this hub observed a counter climb through are booked.
        // Without a baseline under this exact boot there is nothing to subtract
        // from, and a bare reading represents the machine's entire history.
        //
        // The baseline can be missing in three ways, all handled identically. A
        // first report has none. A reading that shrank under the same boot lost
        // one -- an interface included in the sum has disappeared -- so the
        // reading is the remainder of that history and booking it would count it
        // twice. A changed boot_id means the counters restarted or,
        // indistinguishably from here, that a second machine shares the token.
        // Realigning costs the seconds since the reboot; the alternative costs
        // hundreds of gigabytes against a total that only increases.
        //
        // A fourth case: no reading at all. The row is left exactly as it was,
        // since writing zero would realign the baseline to zero and book the next
        // report's lifetime counter as a single delta.
        let (d_rx, d_tx) = match counters {
            None => (0, 0),
            Some(_) if prev_boot.is_empty() || prev_boot != boot_id => {
                // Logged in either case: on a healthy node this is a reboot,
                // while one every few seconds indicates two machines sharing a
                // token.
                if !prev_boot.is_empty() {
                    info!("node {node_id} reports a new boot; re-aligning");
                }
                (0, 0)
            }
            Some((rx, tx)) => ((rx.saturating_sub(last_rx)).max(0), (tx.saturating_sub(last_tx)).max(0)),
        };
        // Saturating rather than a plain `+`: the release profile disables
        // overflow checks, so a total near i64::MAX would wrap to a large
        // negative -- a lifetime figure that has decreased. Two paths reach this
        // column: a node's own counters, which arrive from another repository's
        // binary, and `set_traffic`, through which the panel writes corrections.
        // Clamping here covers both rather than each caller separately.
        total_rx = total_rx.saturating_add(d_rx);
        total_tx = total_tx.saturating_add(d_tx);
        month_rx = month_rx.saturating_add(d_rx);
        month_tx = month_tx.saturating_add(d_tx);
        day_rx = day_rx.saturating_add(d_rx);
        day_tx = day_tx.saturating_add(d_tx);

        // Both boundaries are calendar dates -- the day a provider resets an
        // allowance, the day a person means by "today" -- so both follow the
        // hub's local timezone rather than UTC.
        let period = period_start(Local::now().date_naive(), reset_day).to_string();
        if month_start != period {
            // A new billing period restarts the month counter but not the total.
            month_rx = d_rx;
            month_tx = d_tx;
        }
        let today = Local::now().date_naive().to_string();
        if day_start != today {
            day_rx = d_rx;
            day_tx = d_tx;
        }

        if let Some((rx, tx)) = counters {
            conn.prepare_cached(
                "UPDATE traffic SET boot_id=?2, last_rx=?3, last_tx=?4, total_rx=?5, total_tx=?6,
                                month_rx=?7, month_tx=?8, month_start=?9, day_rx=?10, day_tx=?11,
                                day_start=?12 WHERE node_id=?1",
            )?
            .execute(params![
                node_id, boot_id, rx, tx, total_rx, total_tx, month_rx, month_tx, period, day_rx, day_tx,
                today
            ])?;
        }
        Ok(Traffic { total_rx, total_tx, month_rx, month_tx, month_start: period, day_rx, day_tx })
    }

    /// Allows the panel to correct a total, for example after moving a node to
    /// new hardware.
    ///
    /// The corrected month figures are stamped with the current period; otherwise
    /// they would belong to whichever period the row still held, `all_traffic`
    /// would read them back as zero, and the node's next report would restart the
    /// counter and discard the correction.
    pub fn set_traffic(&self, node_id: i64, p: &TrafficPatch) -> Result<()> {
        let conn = self.conn();
        let reset_day: u32 =
            conn.query_row("SELECT traffic_reset_day FROM node WHERE id=?1", [node_id], |r| r.get(0))?;
        let period = period_start(Local::now().date_naive(), reset_day).to_string();
        conn.execute(
            "UPDATE traffic SET total_rx=COALESCE(?2,total_rx), total_tx=COALESCE(?3,total_tx),
                 month_rx=COALESCE(?4,CASE WHEN month_start=?6 THEN month_rx ELSE 0 END),
                 month_tx=COALESCE(?5,CASE WHEN month_start=?6 THEN month_tx ELSE 0 END), month_start=?6
             WHERE node_id=?1",
            params![node_id, p.total_rx, p.total_tx, p.month_rx, p.month_tx, period],
        )?;
        Ok(())
    }

    // ---- metrics ----

    pub fn insert_metric(&self, node_id: i64, ts: i64, m: &serde_json::Value) -> Result<()> {
        let f = |k: &str| m.get(k).and_then(|v| v.as_f64()).unwrap_or(0.0);
        let n = |k: &str| m.get(k).and_then(|v| v.as_i64()).unwrap_or(0);
        self.conn()
            .prepare_cached(
                "INSERT OR REPLACE INTO metric
               (node_id, ts, cpu, mem_used, swap_used, disk_used, net_rx, net_tx, tcp, udp, procs)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            )?
            .execute(params![
                node_id,
                ts,
                f("cpu"),
                n("mem_used"),
                n("swap_used"),
                n("disk_used"),
                n("net_rx"),
                n("net_tx"),
                n("tcp"),
                n("udp"),
                n("procs")
            ])?;
        Ok(())
    }

    /// History for one node, thinned to one sample every `step` seconds.
    ///
    /// Bucketed rather than filtered on a multiple of `step`: rows normally land
    /// on the minute, but nothing enforces it, and a filter would return nothing
    /// for a stamp falling between grid lines.
    ///
    /// Averaged over the bucket rather than sampled from it. Keeping one row per
    /// bucket would reintroduce the 1/60 sampling the write side already rejects:
    /// the seven-day window integrated to 53.69 GB against the 27.52 GB the
    /// minutes hold, while averaging gives 28.02 GB, matching the accumulator.
    ///
    /// `swap_used`, `tcp`, `udp` and `procs` are stored but not returned, as
    /// nothing draws them from history. The columns are retained deliberately;
    /// `load1` was the fifth and has been removed, see `migrate_to_2`.
    ///
    /// The stamp is the bucket's start rather than a row inside it, so every
    /// series lands on one grid and the probe rows below can be shared.
    pub fn metrics(&self, node_id: i64, since: i64, step: i64) -> Result<Vec<serde_json::Value>> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(
            "SELECT (MIN(ts)/?3)*?3, AVG(cpu), CAST(AVG(mem_used) AS INTEGER),
                    CAST(AVG(disk_used) AS INTEGER),
                    CAST(AVG(net_rx) AS INTEGER), CAST(AVG(net_tx) AS INTEGER)
             FROM metric WHERE node_id=?1 AND ts>=?2 GROUP BY ts/?3 ORDER BY ts/?3",
        )?;
        let rows = stmt.query_map(params![node_id, since, step], |r| {
            Ok(serde_json::json!({
                "ts": r.get::<_, i64>(0)?, "cpu": r.get::<_, f64>(1)?,
                "mem_used": r.get::<_, i64>(2)?, "disk_used": r.get::<_, i64>(3)?,
                "net_rx": r.get::<_, i64>(4)?, "net_tx": r.get::<_, i64>(5)?,
            }))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Drops history beyond the retention window. Traffic totals live in their
    /// own table precisely so history can be pruned freely.
    pub fn prune(&self, keep_days: i64) -> Result<usize> {
        let cutoff = Utc::now().timestamp() - keep_days * 86_400;
        let conn = self.conn();
        let a = conn.execute("DELETE FROM metric WHERE ts < ?1", [cutoff])?;
        let b = conn.execute("DELETE FROM ping_record WHERE ts < ?1", [cutoff])?;
        Ok(a + b)
    }

    // ---- ping ----

    pub fn ping_tasks(&self) -> Result<Vec<PingTask>> {
        let conn = self.conn();
        let mut stmt = conn.prepare("SELECT id, name, target, interval FROM ping_task ORDER BY id")?;
        let tasks: Vec<PingTask> = stmt
            .query_map([], |r| {
                Ok(PingTask {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    target: r.get(2)?,
                    interval: r.get(3)?,
                    nodes: Vec::new(),
                })
            })?
            .collect::<Result<_, _>>()?;
        drop(stmt);
        let mut stmt = conn.prepare("SELECT node_id FROM ping_node WHERE task_id=?1")?;
        tasks
            .into_iter()
            .map(|mut t| {
                t.nodes = stmt.query_map([t.id], |r| r.get(0))?.collect::<Result<_, _>>()?;
                Ok(t)
            })
            .collect()
    }

    /// The maximum number of probes one node may be assigned.
    ///
    /// The agent enforces the same limit: `MAX_PING_TASKS` in that repository
    /// caps the list it will run, since a compromised or buggy hub could
    /// otherwise ask a node for hundreds of outbound connects per second. That
    /// cap is a defence and remains, but on its own it truncates silently,
    /// leaving one line in the node's journal while the hub continues pushing
    /// probes that never run and drawing charts that stay empty.
    ///
    /// The hub knows the total, so the hub issues the refusal. The two must stay
    /// in step; the agent's copy is the backstop rather than the message.
    const MAX_PROBES_PER_NODE: i64 = 64;

    /// The assignments are replaced wholesale, so they run in one transaction:
    /// failing between the delete and the inserts would unassign every node from
    /// a probe the panel still lists them under.
    pub fn save_ping_task(&self, t: &PingTask) -> Result<i64> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let id = if t.id > 0 {
            tx.execute(
                "UPDATE ping_task SET name=?2, target=?3, interval=?4 WHERE id=?1",
                params![t.id, t.name, t.target, t.interval],
            )?;
            t.id
        } else {
            tx.execute(
                "INSERT INTO ping_task (name, target, interval) VALUES (?1,?2,?3)",
                params![t.name, t.target, t.interval],
            )?;
            tx.last_insert_rowid()
        };
        tx.execute("DELETE FROM ping_node WHERE task_id=?1", [id])?;
        for node in &t.nodes {
            // The foreign key is the check; naming the node turns SQLite's
            // "FOREIGN KEY constraint failed" into something the panel can show.
            tx.execute("INSERT INTO ping_node (task_id, node_id) VALUES (?1,?2)", params![id, node])
                .with_context(|| format!("节点 {node} 不存在"))?;
        }
        // Queried from the table after the rows are in rather than counted from
        // the request: an update replaces this task's own assignments, so
        // arithmetic on the way in would have to subtract them again. The
        // transaction makes this atomic with the write, and bailing here rolls it
        // back.
        let crowded: Option<i64> = tx
            .query_row(
                "SELECT node_id FROM ping_node GROUP BY node_id HAVING COUNT(*) > ?1 LIMIT 1",
                [Self::MAX_PROBES_PER_NODE],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(node) = crowded {
            anyhow::bail!(
                "节点 {node} 会被分配超过 {} 个探测任务，agent 最多只跑这么多，多出来的会被静默丢掉",
                Self::MAX_PROBES_PER_NODE
            );
        }
        tx.commit()?;
        Ok(id)
    }

    /// Deletes a probe and the results filed under it.
    ///
    /// `ping_record` carries no foreign key -- it is WITHOUT ROWID and keyed for
    /// the chart query -- so it is cleared explicitly, as in `delete_node`.
    /// SQLite reassigns a deleted probe's id to the next one created, and the
    /// chart selects on `task_id IN (assignments for this node)`: without this
    /// the new probe would draw the removed one's latency under its own name,
    /// with its timeouts folded into the loss figure.
    ///
    /// The delete is a scan -- the key begins at `node_id` -- comparable in cost
    /// to `prune`, for an action taken manually a few times a year.
    pub fn delete_ping_task(&self, id: i64) -> Result<()> {
        let conn = self.conn();
        conn.execute("DELETE FROM ping_record WHERE task_id = ?1", [id])?;
        conn.execute("DELETE FROM ping_task WHERE id=?1", [id])?;
        Ok(())
    }

    /// The task list pushed to one agent.
    ///
    /// Ordered, because the agent keeps the first [`Self::MAX_PROBES_PER_NODE`]
    /// as its backstop against a hub requesting hundreds. Unordered, a list at
    /// that boundary could yield a different subset on each push, restarting half
    /// the timers each time; `save_ping_task` prevents reaching that boundary,
    /// and this makes the backstop deterministic should a database arrive there
    /// by another route.
    pub fn ping_tasks_for(&self, node_id: i64) -> Result<Vec<serde_json::Value>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT t.id, t.target, t.interval FROM ping_task t
             JOIN ping_node n ON n.task_id = t.id WHERE n.node_id = ?1 ORDER BY t.id",
        )?;
        let rows = stmt.query_map([node_id], |r| {
            Ok(serde_json::json!({
                "id": r.get::<_, i64>(0)?, "target": r.get::<_, String>(1)?,
                "interval": r.get::<_, i64>(2)?
            }))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Probe names keyed by id, for labelling one node's latency chart. Names
    /// only: targets and node assignments remain behind `Admin`.
    ///
    /// Restricted to the probes assigned to that node, the only ones its chart
    /// has samples to label. A probe name is operator-supplied text that
    /// routinely carries a hostname or a customer, and the rest of the table
    /// belongs to nodes this caller may not be able to see.
    pub fn ping_task_names(&self, node_id: i64) -> Result<serde_json::Value> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT id, name FROM ping_task WHERE id IN (SELECT task_id FROM ping_node WHERE node_id=?1)",
        )?;
        let rows = stmt.query_map([node_id], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))?;
        let mut names = serde_json::Map::new();
        for row in rows {
            let (id, name) = row?;
            names.insert(id.to_string(), serde_json::json!(name));
        }
        Ok(serde_json::Value::Object(names))
    }

    /// Files one probe result, and only under a probe this node is assigned. A
    /// result for anything else is dropped rather than treated as an error, since
    /// the agent can do nothing useful with the distinction.
    ///
    /// The assignment is tested inside the statement because that is the only
    /// place it is atomic with the write: `ping_record` carries no foreign key,
    /// being WITHOUT ROWID and keyed for the chart query. Two cases arrive
    /// without an assignment. A result already in flight when the panel deleted
    /// its probe, which would otherwise land after `delete_ping_task` swept the
    /// history and be inherited by whichever probe SQLite assigns the id to next.
    /// And a node token in the wrong hands: every other write an agent can cause
    /// is bounded -- one `metric` row per node per minute, one `traffic` row per
    /// node -- while `task_id` is chosen by the reporter, making this the one
    /// write whose row count would otherwise be unbounded.
    ///
    /// The chart's `task_id IN (assignments)` filter hides both afterwards, but
    /// does not prevent the write, its storage, or the id being reused.
    pub fn insert_ping(&self, node_id: i64, task_id: i64, ts: i64, latency: i64) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO ping_record (node_id, task_id, ts, latency)
             SELECT ?1, ?2, ?3, ?4
             WHERE EXISTS (SELECT 1 FROM ping_node WHERE task_id = ?2 AND node_id = ?1)",
            params![node_id, task_id, ts, latency],
        )?;
        Ok(())
    }

    /// Probe results for one node, one sample per probe per `step` seconds: the
    /// bucket's median round trip, its range, and the proportion lost.
    ///
    /// These stamps fall wherever the probe finished rather than on a minute, so
    /// the thinning buckets them instead of matching a multiple, as in `metrics`
    /// above. This is the larger half of that response, since a probe reports far
    /// more often than once a minute.
    ///
    /// [`PING_ROWS`] returns rows in time order, so a bucket is complete the
    /// moment the next opens and only one is held at a time -- at most the probes
    /// assigned to the node times the results one bucket spans.
    ///
    /// Returns the buckets and, alongside them, the proportion of the whole
    /// window each probe lost. The latter cannot be recovered from the former:
    /// [`close_bucket`] divides within each bucket and keeps only the quotient,
    /// so averaging those percentages would weight a bucket holding one sample
    /// equally with one holding twelve. The buckets are necessarily unequal --
    /// the window's first and last are partial by construction, and a probe that
    /// starts, stops, loses its node or skips a round produces more. The
    /// denominators are available only here, in the pass that already reads every
    /// row. Probes that lost nothing are omitted, as `loss` is per bucket.
    pub fn ping_records(
        &self,
        node_id: i64,
        since: i64,
        step: i64,
    ) -> Result<(Vec<serde_json::Value>, serde_json::Value)> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(PING_ROWS)?;
        let mut rows = stmt.query(params![node_id, since, step])?;
        let mut out = Vec::new();
        // Per probe in the bucket being filled: what answered, and how many did
        // not.
        let mut open: Vec<(i64, Vec<i64>, i64)> = Vec::new();
        // Per probe across the whole window: how many were lost, out of how many.
        // Folded in the same pass rather than queried from SQLite a second time,
        // for the same reason the bucket fold itself is in Rust.
        let mut totals: HashMap<i64, (i64, i64)> = HashMap::new();
        let mut bucket = 0;
        while let Some(row) = rows.next()? {
            let (b, task, latency) = (row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?);
            if b != bucket {
                close_bucket(&mut out, &mut open, bucket * step);
                bucket = b;
            }
            let seen = totals.entry(task).or_insert((0, 0));
            seen.1 += 1;
            let probe = match open.iter().position(|(id, ..)| *id == task) {
                Some(at) => &mut open[at],
                None => {
                    open.push((task, Vec::new(), 0));
                    open.last_mut().expect("just pushed")
                }
            };
            // A timeout is stored as -1: excluded from the median and counted
            // instead.
            if latency < 0 {
                probe.2 += 1;
                seen.0 += 1;
            } else {
                probe.1.push(latency);
            }
        }
        close_bucket(&mut out, &mut open, bucket * step);
        // Unrounded: the caller decides how to render it, and rounding here would
        // turn 0.14% into the 0% that denotes no loss at all.
        let loss: serde_json::Map<String, serde_json::Value> = totals
            .into_iter()
            .filter(|(_, (lost, _))| *lost > 0)
            .map(|(task, (lost, samples))| {
                (task.to_string(), serde_json::json!(100.0 * lost as f64 / samples as f64))
            })
            .collect();
        Ok((out, serde_json::Value::Object(loss)))
    }

    /// Fleet-wide counterpart of [`Self::ping_records`]: one ordered scan yields
    /// every node's buckets and window-wide loss, keyed by node id, so the batch
    /// quality endpoint does not run an N-per-node scan that would hold the write
    /// connection once per node.
    ///
    /// The fold mirrors the per-node one bucket for bucket; only the boundaries
    /// differ (a node change also closes the trailing bucket). The first row
    /// carries `node = i64::MIN`, so the first transition never flushes a
    /// half-built node.
    pub fn ping_records_all(
        &self,
        since: i64,
        step: i64,
    ) -> Result<HashMap<i64, (Vec<serde_json::Value>, serde_json::Value)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare_cached(PING_ROWS_ALL)?;
        let mut rows = stmt.query(params![since, step])?;

        let mut out: HashMap<i64, (Vec<serde_json::Value>, serde_json::Value)> = HashMap::new();
        // Reset each time the scan moves to a new node.
        let mut node = i64::MIN;
        let mut bucket = 0i64;
        let mut buckets: Vec<serde_json::Value> = Vec::new();
        let mut open: Vec<(i64, Vec<i64>, i64)> = Vec::new();
        let mut totals: HashMap<i64, (i64, i64)> = HashMap::new();

        while let Some(row) = rows.next()? {
            let (n, b, task, latency) =
                (row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?);
            if n != node {
                finish_node(&mut out, node, &mut buckets, &mut open, bucket, step, &mut totals);
                node = n;
                bucket = b;
            } else if b != bucket {
                close_bucket(&mut buckets, &mut open, bucket * step);
                bucket = b;
            }
            let seen = totals.entry(task).or_insert((0, 0));
            seen.1 += 1;
            let probe = match open.iter().position(|(id, ..)| *id == task) {
                Some(at) => &mut open[at],
                None => {
                    open.push((task, Vec::new(), 0));
                    open.last_mut().expect("just pushed")
                }
            };
            // A timeout is stored as -1: excluded from the median and counted
            // instead.
            if latency < 0 {
                probe.2 += 1;
                seen.0 += 1;
            } else {
                probe.1.push(latency);
            }
        }
        finish_node(&mut out, node, &mut buckets, &mut open, bucket, step, &mut totals);
        Ok(out)
    }

    // ---- the database file itself ----

    /// The file this connection is open on, empty for `:memory:`.
    pub fn file(&self) -> String {
        main_file(&self.conn())
    }

    /// The retention window used by both `prune` and the data page. Stored as
    /// text by the settings form, so a missing or unparsable value falls back to
    /// the default rather than erroring.
    pub fn retention_days(&self) -> i64 {
        self.get("retention_days").and_then(|v| v.parse::<i64>().ok()).unwrap_or(7).clamp(1, 3_650)
    }

    /// What the panel's data page reads: how much space the file occupies, how
    /// much of that is free pages awaiting a `VACUUM`, and how far back the
    /// history actually reaches.
    ///
    /// `oldest` against `retention` is the one pair here that can indicate a
    /// fault: history older than the window means `prune` has not been running.
    pub fn stats(&self) -> Result<serde_json::Value> {
        // Before acquiring the connection: `conn()` returns a guard on a plain
        // Mutex, and `retention_days` acquires the same one.
        let retention = self.retention_days();
        let conn = self.conn();
        let file = main_file(&conn);
        let page_size: i64 = conn.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let free_pages: i64 = conn.query_row("PRAGMA freelist_count", [], |r| r.get(0))?;
        // Both are pruned at the same cutoff, so the earlier of the two marks
        // where history begins. A full scan of each, which the counts below
        // already incur.
        let oldest: Option<i64> = conn.query_row(
            "SELECT MIN(ts) FROM (SELECT MIN(ts) AS ts FROM metric UNION ALL SELECT MIN(ts) FROM ping_record)",
            [],
            |r| r.get(0),
        )?;
        let mut rows = serde_json::Map::new();
        for table in TABLES {
            let n: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))?;
            rows.insert(table.to_owned(), serde_json::json!(n));
        }
        // 插件空间占用(R11/KTD11):按插件汇总 plugin_data 的字节/行数,再加上
        // 该插件的 kv 行(setting 表的 `plugin.<id>:%`)字节。宿主只展示,
        // 清理交给插件自己的 cleanup 入口。查询直接走当前 conn——`plugin_data_usage`
        // 会再取同一把 Mutex,在持锁期间调用会死锁。
        let mut plugins = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT plugin_id, name FROM plugin ORDER BY plugin_id")?;
            let listed = stmt
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
                .collect::<Result<Vec<_>, _>>()?;
            for (plugin_id, name) in listed {
                let (data_rows, data_bytes) = conn.query_row(
                    "SELECT COUNT(*), COALESCE(SUM(LENGTH(CAST(data AS BLOB))),0) FROM plugin_data WHERE plugin_id=?1",
                    params![plugin_id],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
                )?;
                let kv_bytes: i64 = conn.query_row(
                    "SELECT COALESCE(SUM(LENGTH(key)+LENGTH(value)),0) FROM setting WHERE key LIKE ?1 ESCAPE '\\'",
                    params![format!("plugin.{}:%", crate::db_plugins::like_escaped(&plugin_id))],
                    |r| r.get(0),
                )?;
                plugins.push(serde_json::json!({
                    "plugin_id": plugin_id,
                    "name": name,
                    "data_rows": data_rows,
                    "data_bytes": data_bytes,
                    "kv_bytes": kv_bytes,
                }));
            }
        }
        Ok(serde_json::json!({
            "path": file,
            "size": bytes_of(&file),
            "wal": bytes_of(&format!("{file}-wal")),
            "free": free_pages * page_size,
            "oldest": oldest,
            "retention": retention,
            "rows": rows,
            "plugins": plugins,
        }))
    }

    /// Writes a consistent copy of the live database to `dest`, which must not
    /// already exist.
    ///
    /// `VACUUM INTO` is SQLite's own mechanism for this: one statement, a single
    /// read transaction, and a compacted copy with free pages already dropped. It
    /// reads the whole file, so the caller runs it off the runtime -- every other
    /// statement here is sub-millisecond, this one is not.
    pub fn backup_into(&self, dest: &str) -> Result<()> {
        // A second connection to the same file. `VACUUM INTO` only reads, and WAL
        // allows it to read a consistent snapshot while the agents continue
        // writing through the first -- exporting is the one heavy operation here
        // that need not block them. A fresh connection inherits none of the
        // PRAGMAs in SCHEMA, so the busy timeout must be set again or a
        // checkpoint racing this read returns SQLITE_BUSY immediately.
        let reader = Connection::open(self.file())?;
        reader.busy_timeout(std::time::Duration::from_secs(5))?;
        reader.execute("VACUUM INTO ?1", [dest])?;
        // The copy is the credential store in one portable file: node tokens in
        // the clear, the GitHub secret, the password hash. SQLite creates it
        // under the umask, which at the usual 022 is world-readable.
        own_only(dest);
        Ok(())
    }

    /// Rebuilds the file, reclaiming the pages deleted history left behind.
    /// Returns the bytes recovered.
    ///
    /// SQLite's constraints on `VACUUM`, and why they hold here: it cannot run
    /// inside a transaction or with a live statement on the connection (there is
    /// one connection, and this call owns it); it requires roughly as much free
    /// disk as the database itself, and a failure rolls back leaving the original
    /// untouched; and it can renumber rowids, which nothing here keys on, since
    /// `metric` and `ping_record` are WITHOUT ROWID and every other table
    /// declares its own primary key.
    ///
    /// In WAL mode the rewrite lands in the WAL first, so without the checkpoint
    /// the file on disk grows rather than shrinking.
    pub fn vacuum(&self) -> Result<i64> {
        let conn = self.conn();
        let file = main_file(&conn);
        let before = on_disk(&file);
        conn.execute_batch("VACUUM")?;
        // Best effort: the space is already reclaimed within the database, and a
        // checkpoint that cannot run now does not constitute a failed vacuum.
        let _ = conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        Ok((before - on_disk(&file)).max(0))
    }

    /// What a file must satisfy before a single page of it is copied over the
    /// live database. Restore is the one operation here that destroys data, and
    /// the file behind it originates from a disk this hub knows nothing about.
    ///
    /// **Writes to `src`.** The migrations an older backup requires run here, on
    /// the upload, rather than after copying: everything that can fail does so
    /// while the live database is still untouched. The caller owns that file and
    /// deletes it in either case.
    pub fn check_backup(&self, src: &str) -> Result<()> {
        // Read-write rather than read-only: a plain copy of a running hub's
        // database is in WAL mode, and SQLite cannot open such a file read-only
        // without its -shm companion.
        let candidate = Connection::open(src)?;
        let health: String = candidate
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(|e| anyhow::anyhow!("not a readable SQLite database: {e}"))?;
        if health != "ok" {
            anyhow::bail!("the file is a damaged database: {health}");
        }
        // Pages are copied verbatim, so whatever schema the file carries becomes
        // the schema this hub runs its statements against. A view or trigger
        // where a table belongs would route every subsequent write through
        // externally supplied code.
        let plotted: i64 = candidate.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('view', 'trigger')",
            [],
            |r| r.get(0),
        )?;
        if plotted > 0 {
            anyhow::bail!("the file carries views or triggers, which a hub backup never does");
        }
        for table in &TABLES[..8] {
            let found: i64 = candidate.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get(0),
            )?;
            if found == 0 {
                anyhow::bail!("the file is not a hub backup: no {table} table");
            }
        }
        let version: i64 = candidate.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if version > SCHEMA_VERSION {
            anyhow::bail!(
                "the backup is from a newer hub (schema {version}, this one reads {SCHEMA_VERSION}); upgrade first"
            );
        }
        // The online backup API refuses a page size change while the destination
        // is in WAL mode; an explicit message is clearer than SQLITE_READONLY.
        let theirs: i64 = candidate.query_row("PRAGMA page_size", [], |r| r.get(0))?;
        let ours: i64 = self.conn().query_row("PRAGMA page_size", [], |r| r.get(0))?;
        if theirs != ours {
            anyhow::bail!("the backup uses a {theirs}-byte page, this database uses {ours}");
        }
        // Brought up to this build's schema here, on the upload. Run after the
        // copy instead, a failed migration would leave the hub on a database it
        // could not use while reporting a failure to the panel -- the one
        // arrangement in which the restore has failed and the original data is
        // also gone.
        migrate(&candidate, version)?;
        // The tables a migration adds are checked after it has run: a v3
        // backup arrives without them, and it is exactly the file the migration
        // exists to fix. Everything a pre-v4 hub wrote is checked before the
        // migration touches the file, so a file missing those is still refused
        // before anything is written.
        for table in &TABLES[8..] {
            let found: i64 = candidate.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                [table],
                |r| r.get(0),
            )?;
            if found == 0 {
                anyhow::bail!("the file is not a hub backup: no {table} table");
            }
        }
        // The migration lands in a -wal beside a backup taken from a running hub.
        // Checkpointed here so the copy below reads a single file.
        let _ = candidate.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));

        // Table names are not a schema. Pages are copied verbatim, so the columns
        // the file carries become the ones this hub's statements run against, and
        // eight correctly named tables holding the wrong columns pass every gate
        // above while leaving the database unusable.
        //
        // Compared against a database this build creates for itself, so there is
        // no second column list to keep in step with `SCHEMA`. Names are compared
        // as sets rather than as stored DDL: a migrated old backup reaches the
        // same columns through `ALTER TABLE`, whose text never matches a fresh
        // `CREATE TABLE`. Extra columns are ignored.
        let reference = Connection::open_in_memory()?;
        reference.execute_batch(SCHEMA)?;
        migrate(&reference, SCHEMA_VERSION)?;
        for table in TABLES {
            let want = columns_of(&reference, table)?;
            let got = columns_of(&candidate, table)?;
            let mut missing: Vec<&str> = want.difference(&got).map(String::as_str).collect();
            if !missing.is_empty() {
                missing.sort_unstable();
                anyhow::bail!("the file's {table} table is missing {}", missing.join(", "));
            }
        }
        Ok(())
    }

    /// Copies a checked backup over the live database page by page through
    /// SQLite's online backup API: the destination retains its file, permissions
    /// and journal mode, and a partial failure rolls back rather than leaving
    /// half a database behind.
    ///
    /// Call [`Db::check_backup`] first, as it is what brings `src` to this
    /// build's schema; the copy is then the last step and nothing after it can
    /// fail. Like the other two, this reads and writes the whole file and belongs
    /// off the runtime.
    pub fn restore_from(&self, src: &str) -> Result<()> {
        let mut conn = self.conn();
        conn.restore(rusqlite::MAIN_DB, src, None::<fn(rusqlite::backup::Progress)>)?;
        Ok(())
    }

    // ---- sessions ----

    pub fn create_session(&self, token_hash: &str, expires_at: i64) -> Result<()> {
        self.conn().execute(
            "INSERT OR REPLACE INTO session (token_hash, expires_at) VALUES (?1, ?2)",
            params![token_hash, expires_at],
        )?;
        Ok(())
    }

    pub fn session_valid(&self, token_hash: &str) -> bool {
        self.conn()
            .query_row(
                "SELECT 1 FROM session WHERE token_hash=?1 AND expires_at > ?2",
                params![token_hash, Utc::now().timestamp()],
                |_| Ok(()),
            )
            .optional()
            .ok()
            .flatten()
            .is_some()
    }

    /// Live sessions, newest first. Expired rows are filtered here rather than
    /// left to `expire_sessions`, which sweeps only once an hour.
    pub fn sessions(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT token_hash, expires_at FROM session WHERE expires_at > ?1 ORDER BY expires_at DESC",
        )?;
        let rows = stmt
            .query_map([Utc::now().timestamp()], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    pub fn drop_session(&self, token_hash: &str) -> Result<()> {
        self.conn().execute("DELETE FROM session WHERE token_hash=?1", [token_hash])?;
        Ok(())
    }

    /// Invalidates every login. Used when the admin password changes.
    pub fn drop_all_sessions(&self) -> Result<()> {
        self.conn().execute("DELETE FROM session", [])?;
        Ok(())
    }

    pub fn expire_sessions(&self) -> Result<()> {
        self.conn().execute("DELETE FROM session WHERE expires_at <= ?1", [Utc::now().timestamp()])?;
        Ok(())
    }
}

/// Turns one finished bucket into a row per probe, stamped with the bucket's
/// start so every series lands on the same grid.
///
/// Median rather than mean: one SYN retransmit is tens of milliseconds and would
/// drag a mean, and it is the reading that is wrong rather than the link.
///
/// `latency` is null when an entire bucket timed out. `loss` is the percentage
/// that did, included only when non-zero -- a healthy day is 2,880 rows, and
/// `"loss":0` on each would add 29 kB of nothing. Rounded up, so that the absence
/// of a `loss` key means no timeouts occurred: truncating would report a bucket
/// that lost 1 of 180 as clean.
fn close_bucket(out: &mut Vec<serde_json::Value>, open: &mut Vec<(i64, Vec<i64>, i64)>, ts: i64) {
    // Ordered by probe rather than by which answered first in this bucket, since
    // the chart shades its lines by arrival order.
    open.sort_unstable_by_key(|(task, ..)| *task);
    for (task, mut answered, lost) in open.drain(..) {
        answered.sort_unstable();
        let middle = match answered.len() {
            0 => None,
            n if n % 2 == 1 => Some(answered[n / 2]),
            n => Some((answered[n / 2 - 1] + answered[n / 2]) / 2),
        };
        let mut row = serde_json::json!({"task_id": task, "ts": ts, "latency": middle});
        // Only when the bucket actually varied. At the hour and six-hour windows a
        // bucket holds one sample, and a band would be a zero-height ribbon under
        // every line.
        if let (Some(lo), Some(hi)) = (answered.first(), answered.last()) {
            if hi > lo {
                row["band"] = serde_json::json!([lo, hi]);
            }
        }
        if lost > 0 {
            let total = answered.len() as i64 + lost;
            row["loss"] = ((100 * lost + total - 1) / total).into();
        }
        out.push(row);
    }
}

/// Closes a node's trailing bucket and folds its window-wide loss, then stores
/// the pair under `node`. The per-node counterpart of this is the tail of
/// [`Db::ping_records`]; the fleet-wide scan calls it once per node since a node
/// boundary also ends the last bucket.
///
/// The sentinel `node == i64::MIN` (no rows seen yet) inserts nothing, so the
/// first transition is a no-op rather than a bogus entry.
#[allow(clippy::too_many_arguments)]
fn finish_node(
    out: &mut HashMap<i64, (Vec<serde_json::Value>, serde_json::Value)>,
    node: i64,
    buckets: &mut Vec<serde_json::Value>,
    open: &mut Vec<(i64, Vec<i64>, i64)>,
    bucket: i64,
    step: i64,
    totals: &mut HashMap<i64, (i64, i64)>,
) {
    if node == i64::MIN {
        return;
    }
    close_bucket(buckets, open, bucket * step);
    // Unrounded, matching `ping_records`: rounding here would turn 0.14% into the
    // 0% that denotes no loss at all.
    let loss: serde_json::Map<String, serde_json::Value> = totals
        .drain()
        .filter(|(_, (lost, _))| *lost > 0)
        .map(|(task, (lost, samples))| {
            (task.to_string(), serde_json::json!(100.0 * lost as f64 / samples as f64))
        })
        .collect();
    out.insert(node, (std::mem::take(buckets), serde_json::Value::Object(loss)));
}

fn row_to_node(r: &rusqlite::Row<'_>) -> Node {
    let s = |i: &str| r.get::<_, String>(i).unwrap_or_default();
    let n = |i: &str| r.get::<_, i64>(i).unwrap_or(0);
    Node {
        id: n("id"),
        name: s("name"),
        public: r.get::<_, bool>("public").unwrap_or(true),
        sort: n("sort"),
        remark: s("remark"),
        traffic_limit: n("traffic_limit"),
        traffic_mode: s("traffic_mode"),
        traffic_reset_day: n("traffic_reset_day") as u32,
        hostname: s("hostname"),
        os: s("os"),
        kernel: s("kernel"),
        arch: s("arch"),
        virt: s("virt"),
        cpu_name: s("cpu_name"),
        cpu_cores: n("cpu_cores"),
        mem_total: n("mem_total"),
        swap_total: n("swap_total"),
        disk_total: n("disk_total"),
        agent_version: s("agent_version"),
        ip: s("ip"),
        ipv4: s("ipv4"),
        ipv6: s("ipv6"),
        observed_ip: s("observed_ip"),
        country: s("country"),
        last_seen: n("last_seen"),
        token: s("token"),
    }
}

/// Start of the billing period containing `today`, given a reset day of month.
/// A reset day past the end of a short month lands on that month's last day.
pub fn period_start(today: NaiveDate, reset_day: u32) -> NaiveDate {
    let day = reset_day.clamp(1, 31);
    let clamped = |y: i32, m: u32| {
        let last =
            NaiveDate::from_ymd_opt(if m == 12 { y + 1 } else { y }, if m == 12 { 1 } else { m + 1 }, 1)
                .unwrap()
                .pred_opt()
                .unwrap()
                .day();
        NaiveDate::from_ymd_opt(y, m, day.min(last)).unwrap()
    };
    let this = clamped(today.year(), today.month());
    if today >= this {
        this
    } else if today.month() == 1 {
        clamped(today.year() - 1, 12)
    } else {
        clamped(today.year(), today.month() - 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PRAGMA settings are per connection, so a value read through any other
    /// handle proves nothing about the one the hub writes through.
    #[test]
    fn the_tuning_pragmas_reach_the_connection_the_hub_uses() {
        let db = db();
        let conn = db.conn();
        let read = |p: &str| conn.query_row(&format!("PRAGMA {p}"), [], |r| r.get::<_, i64>(0)).unwrap();
        assert_eq!(read("cache_size"), -8192, "8 MiB of page cache");
        assert_eq!(read("wal_autocheckpoint"), 256);
        assert_eq!(read("journal_size_limit"), 1_048_576);
        assert_eq!(read("busy_timeout"), 5_000);
    }

    /// `try_get` 与 `get` 的差别只有一处:读库失败时前者传错、后者吞成 None。
    /// 要区分「行不存在」与「库坏了」的调用方靠这个差别决定报 400/403 还是 500。
    #[test]
    fn try_get_separates_a_read_failure_from_a_missing_row() {
        let db = db();
        db.set("present", "value").unwrap();
        assert_eq!(db.try_get("present").unwrap().as_deref(), Some("value"));
        assert_eq!(db.try_get("absent").unwrap(), None, "行不存在是 Ok(None),不是错");
        assert_eq!(db.get("absent"), None);
        // 制造一次真实的读库失败:表没了。两条路在这一刻分道扬镳。
        db.conn().execute("DROP TABLE setting", []).unwrap();
        assert!(db.try_get("present").is_err(), "读库失败要传错");
        assert_eq!(db.get("present"), None, "而 get 把它吞成「没有这一行」");
    }

    /// A real file, since these three tests exist to exercise what happens to
    /// one. Removed by the test that created it.
    struct Scratch(String);

    impl Scratch {
        fn new() -> Self {
            Self(
                std::env::temp_dir()
                    .join(format!("monitor-test-{}.db", rand::random::<u64>()))
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm", ".copy"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", self.0));
            }
        }
    }

    /// Backup and restore are the two operations that can lose every row in the
    /// database, so this exercises the whole path: take a copy, modify the live
    /// database, restore the copy, and confirm the change is gone.
    #[test]
    fn a_backup_restores_the_database_it_was_taken_from() {
        let scratch = Scratch::new();
        let copy = format!("{}.copy", scratch.0);
        let db = Db::open(&scratch.0).unwrap();
        let kept =
            db.create_node(&Node { name: "backed-up".into(), ..Default::default() }, "token-kept").unwrap();
        db.backup_into(&copy).unwrap();

        // Everything after the copy must disappear on restore, including a node
        // that reclaimed the deleted one's id.
        db.delete_node(kept).unwrap();
        db.create_node(&Node { name: "after".into(), ..Default::default() }, "token-after").unwrap();

        db.check_backup(&copy).unwrap();
        db.restore_from(&copy).unwrap();
        let back = db.nodes().unwrap();
        assert_eq!(back.len(), 1);
        assert_eq!((back[0].name.as_str(), back[0].token.as_str()), ("backed-up", "token-kept"));
        assert!(db.node_by_token("token-after").unwrap().is_none(), "the row made after the copy is gone");

        // The connection remains the hub's: it can write, it is on the schema this
        // build expects, and it retains the journal mode the hub opened with --
        // the copy `VACUUM INTO` wrote is not in WAL mode.
        node(&db, 1);
        let conn = db.conn();
        assert_eq!(
            conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(),
            SCHEMA_VERSION
        );
        assert_eq!(conn.query_row("PRAGMA journal_mode", [], |r| r.get::<_, String>(0)).unwrap(), "wal");
        drop(conn);
        let _ = std::fs::remove_file(&copy);
    }

    /// The upload behind restore is an externally supplied file. Each case here
    /// is a way for it not to be a hub backup, and every one must be caught
    /// before a single page is copied over live data.
    #[test]
    fn restore_refuses_anything_that_is_not_a_backup_of_this_hub() {
        let scratch = Scratch::new();
        let db = Db::open(&scratch.0).unwrap();
        let bad = format!("{}.copy", scratch.0);

        std::fs::write(&bad, b"this is not a database at all").unwrap();
        assert!(db.check_backup(&bad).is_err(), "not SQLite");

        let _ = std::fs::remove_file(&bad);
        let empty = Connection::open(&bad).unwrap();
        empty.execute_batch("CREATE TABLE unrelated (a)").unwrap();
        assert!(db.check_backup(&bad).is_err(), "SQLite, but not this schema");

        // A file carrying its own code where a table belongs: the restore copies
        // pages, so that schema would become the one the hub runs every statement
        // against.
        empty.execute_batch(&SCHEMA.replace("PRAGMA journal_mode = WAL;", "")).unwrap();
        empty
            .execute_batch(
                "DROP TABLE session; CREATE VIEW session AS SELECT 1 AS token_hash, 2 AS expires_at",
            )
            .unwrap();
        assert!(db.check_backup(&bad).is_err(), "a view where a table belongs");

        // Eight tables with the right names and none of the right columns. Every
        // gate above passes: it is a healthy SQLite file, it carries no view or
        // trigger, all eight names are present, it stamps itself with this build's
        // version and uses the same page size. Restoring copies pages, so those
        // columns would become the ones the hub runs every statement against,
        // leaving the panel reporting a failed restore over a database already
        // replaced.
        let _ = std::fs::remove_file(&bad);
        let shaped = Connection::open(&bad).unwrap();
        for table in TABLES {
            shaped.execute_batch(&format!("CREATE TABLE {table} (x TEXT)")).unwrap();
        }
        shaped.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}")).unwrap();
        // Which table fails first follows the order of TABLES and is incidental;
        // naming the table and the columns is what matters.
        let refused = db.check_backup(&bad).unwrap_err().to_string();
        assert!(refused.contains("table is missing"), "{refused}");

        // From a hub carrying a schema this build has never seen.
        let _ = std::fs::remove_file(&bad);
        let newer = Connection::open(&bad).unwrap();
        newer.execute_batch(SCHEMA).unwrap();
        newer.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1)).unwrap();
        assert!(db.check_backup(&bad).is_err(), "from a newer hub");

        newer.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}")).unwrap();
        db.check_backup(&bad).unwrap();
    }

    /// `oldest` is what the data page compares against the retention window, so it
    /// must span both pruned tables rather than whichever happens to have rows.
    #[test]
    fn stats_report_the_earliest_history_row_and_the_window_it_is_kept_for() {
        let scratch = Scratch::new();
        let db = Db::open(&scratch.0).unwrap();
        let id = node(&db, 1);
        let now = Utc::now().timestamp();

        assert_eq!(db.stats().unwrap()["oldest"], serde_json::Value::Null, "no history, no start");
        assert_eq!(db.stats().unwrap()["retention"], 7, "an unset window is the default");

        db.insert_metric(id, now - 3 * 86_400, &serde_json::json!({"cpu": 1.0})).unwrap();
        assert_eq!(db.stats().unwrap()["oldest"], now - 3 * 86_400);

        // Older, and in the other table: the earlier of the two prevails. The probe
        // must be assigned, or the result is not this node's to file.
        let task = db
            .save_ping_task(&PingTask {
                id: 0,
                name: "p".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes: vec![id],
            })
            .unwrap();
        db.insert_ping(id, task, now - 9 * 86_400, 12).unwrap();
        assert_eq!(db.stats().unwrap()["oldest"], now - 9 * 86_400);

        db.set("retention_days", "9999").unwrap();
        assert_eq!(db.stats().unwrap()["retention"], 3_650, "a stored window is still clamped");
    }

    /// Deleted rows leave free pages behind; only a rebuild returns them to the
    /// filesystem, and in WAL mode only after the checkpoint.
    #[test]
    fn vacuum_gives_the_deleted_pages_back_to_the_filesystem() {
        let scratch = Scratch::new();
        let db = Db::open(&scratch.0).unwrap();
        let id = node(&db, 1);
        let now = Utc::now().timestamp();
        let sample = serde_json::json!({"cpu": 1.0, "mem_used": 1, "swap_used": 1, "disk_used": 1,
            "net_rx": 1, "net_tx": 1, "tcp": 1, "udp": 1, "procs": 1});
        // Every row strictly before `now`: `prune(0)` cuts at its own `Utc::now()`,
        // and `ts < cutoff` would spare a row stamped in the same second the prune
        // runs.
        for i in 1..=20_000 {
            db.insert_metric(id, now - i, &sample).unwrap();
        }
        let _ = db.conn().query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()));
        let fat = on_disk(&scratch.0);
        db.prune(0).unwrap();

        let freed = db.vacuum().unwrap();
        assert!(freed > 0, "a vacuum after deleting 20 000 rows has to return space");
        assert!(on_disk(&scratch.0) < fat);
        assert_eq!(db.stats().unwrap()["rows"]["metric"], 0);
        assert_eq!(db.nodes().unwrap().len(), 1, "vacuum keeps the rows that are left");
    }

    /// 插件空间占用(R11/KTD11):按插件汇总 plugin_data 的字节/行数,并计入
    /// 该插件 kv 行的字节。
    #[test]
    fn stats_reports_per_plugin_usage() {
        let db = db();
        let row = db.create_plugin("com.example.big", "Big", "1", "{}", b"m", "sha").unwrap();
        assert!(row.id > 0);
        db.plugin_data_put("com.example.big", "node:1", &"x".repeat(100)).unwrap();
        db.plugin_data_put("com.example.big", "node:2", &"y".repeat(50)).unwrap();
        db.set("plugin.com.example.big:token", "secret").unwrap();

        let stats = db.stats().unwrap();
        let plugins = stats["plugins"].as_array().unwrap();
        let entry = plugins.iter().find(|p| p["plugin_id"] == "com.example.big").unwrap();
        assert_eq!(entry["data_rows"], 2);
        assert_eq!(entry["data_bytes"], 150);
        assert_eq!(
            entry["kv_bytes"],
            "plugin.com.example.big:token".len() + "secret".len(),
            "kv 行按完整 key（含命名空间前缀）+value 计字节"
        );

        // 删除插件后占用随之消失。
        db.delete_plugin_with_kv(row.id, "com.example.big").unwrap();
        let after = db.stats().unwrap();
        assert!(after["plugins"].as_array().unwrap().is_empty(), "删掉的插件不再出现在占用里");
    }

    fn node(db: &Db, reset_day: u32) -> i64 {
        let token = format!("token-{}", rand::random::<u32>());
        db.create_node(&Node { name: "n".into(), traffic_reset_day: reset_day, ..Default::default() }, &token)
            .unwrap()
    }

    /// The country is derived from the address, so it must be dropped the moment
    /// the address no longer matches -- and only then, or every reconnect would
    /// spend an outbound request repeating a settled lookup.
    #[test]
    fn a_country_outlives_a_reconnect_and_dies_with_the_address_it_came_from() {
        let db = db();
        let id = node(&db, 1);
        let facts = serde_json::json!({"hostname": "h"});
        let save = |ip: &str| db.save_facts(id, &facts, ip, "").unwrap();
        let stored = || db.node(id).unwrap().unwrap().country;

        assert!(save("198.51.100.4"), "a node with no country is owed a lookup");
        db.set_country(id, "US", "198.51.100.4").unwrap();
        assert!(!save("198.51.100.4"), "the same address asks nothing a second time");
        assert_eq!(stored(), "US");
        assert!(save("203.0.113.9"), "a new address is a new question");
        assert_eq!(stored(), "", "and the answer to the old one is gone");

        // A lookup issued for the old address, arriving after the move.
        db.set_country(id, "US", "198.51.100.4").unwrap();
        assert_eq!(stored(), "", "an answer about an address the node has left is dropped");
        db.set_country(id, "JP", "203.0.113.9").unwrap();
        assert_eq!(stored(), "JP", "the answer about the address it is at now lands");
    }

    #[test]
    fn traffic_survives_a_reboot_instead_of_resetting() {
        let db = db();
        let id = node(&db, 1);

        // The first report only establishes the baseline.
        let t = db.accumulate(id, "boot-a", Some((5_000, 3_000))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (0, 0));

        let t = db.accumulate(id, "boot-a", Some((9_000, 6_000))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (4_000, 3_000));

        // Reboot: a new boot_id with counters restarting near zero. The total must
        // not fall back to the fresh value, and the 700 bytes moved before the
        // first report are not booked, nothing having measured them.
        let t = db.accumulate(id, "boot-b", Some((700, 400))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (4_000, 3_000), "a reboot must not reset the total");

        // Counting resumes from the new baseline.
        let t = db.accumulate(id, "boot-b", Some((1_700, 900))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (5_000, 3_500));
        assert_eq!((t.month_rx, t.month_tx), (5_000, 3_500));
    }

    /// One install command pasted onto a second machine: both agents answer for
    /// the same node and evict each other, so the hub sees two boot_ids
    /// alternating, each with its own lifetime counter. Booking those would add
    /// roughly 180 GB per swap to a total that only increases.
    #[test]
    fn two_machines_sharing_one_token_cannot_inflate_the_total() {
        let db = db();
        let id = node(&db, 1);
        let (a, b) = (100_000_000_000, 80_000_000_000); // two lifetime counters

        db.accumulate(id, "boot-a", Some((a, a))).unwrap();
        let t = db.accumulate(id, "boot-a", Some((a + 1_000, a + 1_000))).unwrap();
        assert_eq!(t.total_rx, 1_000, "the real machine's own traffic still counts");

        // Every swap presents a boot_id with no baseline, so every swap books
        // nothing.
        for round in 0..3 {
            db.accumulate(id, "boot-b", Some((b + round, b + round))).unwrap();
            db.accumulate(id, "boot-a", Some((a + 1_000 + round, a + 1_000 + round))).unwrap();
        }
        let t = db.all_traffic()[&id].clone();
        assert!(t.total_rx < 10_000, "six swaps booked {} bytes, not a lifetime counter", t.total_rx);
    }

    #[test]
    fn a_shrinking_reading_re_aligns_instead_of_re_counting_history() {
        let db = db();
        let id = node(&db, 1);
        db.accumulate(id, "boot-a", Some((10_000, 10_000))).unwrap();
        let t = db.accumulate(id, "boot-a", Some((12_000, 12_000))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (2_000, 2_000));

        // The same boot with a reduced reading: an interface included in the sum
        // has gone, so this is the remainder of the machine's history rather than
        // new bytes.
        let t = db.accumulate(id, "boot-a", Some((500, 500))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (2_000, 2_000));

        // Aligned to the smaller baseline, counting resumes from there.
        let t = db.accumulate(id, "boot-a", Some((900, 900))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (2_400, 2_400));

        // A new boot realigns identically, for the same reason: it has no baseline
        // either.
        let t = db.accumulate(id, "boot-b", Some((300, 300))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (2_400, 2_400));

        // One direction shrinking does not deprive the other of its increment.
        let t = db.accumulate(id, "boot-b", Some((100, 900))).unwrap();
        assert_eq!((t.total_rx, t.total_tx), (2_400, 3_000));
    }

    /// The two counters that restart on their own schedules, against a total that
    /// never does. Each derives from its own stored date, so a rollover must leave
    /// the other untouched.
    #[test]
    fn day_and_month_restart_independently_while_the_total_keeps_climbing() {
        let db = db();
        let id = node(&db, 1);
        db.accumulate(id, "boot-a", Some((0, 0))).unwrap();
        let t = db.accumulate(id, "boot-a", Some((8_000, 4_000))).unwrap();
        assert_eq!((t.day_rx, t.day_tx), (8_000, 4_000));
        assert_eq!((t.month_rx, t.month_tx), (8_000, 4_000));

        // Midnight passes, forced through the stored date the rollover reads.
        db.conn().execute("UPDATE traffic SET day_start='1999-01-01' WHERE node_id=?1", [id]).unwrap();
        let t = db.accumulate(id, "boot-a", Some((9_500, 4_600))).unwrap();
        assert_eq!((t.day_rx, t.day_tx), (1_500, 600), "a new day counts only this report's delta");
        assert_eq!(t.month_rx, 9_500, "the month is not a day");
        assert_eq!(t.total_rx, 9_500, "and the total is neither");

        // The billing period then rolls over, partway through that same day.
        db.conn().execute("UPDATE traffic SET month_start='1999-01-01' WHERE node_id=?1", [id]).unwrap();
        let t = db.accumulate(id, "boot-a", Some((10_000, 4_700))).unwrap();
        assert_eq!((t.month_rx, t.month_tx), (500, 100), "a new period counts only this report's delta");
        assert_eq!((t.day_rx, t.day_tx), (2_000, 700), "the day carries on across a billing rollover");
        assert_eq!(t.total_rx, 10_000, "lifetime total is untouched by either rollover");
    }

    /// The other half of the rollover: the counters restart on the node's next
    /// report, so a node silent since before a boundary still holds the previous
    /// period's bytes on disk. The read side must not return those.
    #[test]
    fn a_node_that_went_quiet_before_a_boundary_reads_as_zero_this_period() {
        let db = db();
        let id = node(&db, 1);
        db.accumulate(id, "boot-a", Some((0, 0))).unwrap();
        db.accumulate(id, "boot-a", Some((8_000, 4_000))).unwrap();
        assert_eq!(db.all_traffic()[&id].day_rx, 8_000, "still today, so it still counts");

        // Offline across both boundaries, with no report to restart either.
        db.conn()
            .execute(
                "UPDATE traffic SET day_start='1999-01-01', month_start='1999-01-01' WHERE node_id=?1",
                [id],
            )
            .unwrap();
        let t = db.all_traffic()[&id].clone();
        assert_eq!((t.day_rx, t.day_tx), (0, 0), "yesterday's bytes are not today's");
        assert_eq!((t.month_rx, t.month_tx), (0, 0), "last period's bytes are not this period's");
        assert_eq!(t.month_start, period_start(Local::now().date_naive(), 1).to_string());
        assert_eq!((t.total_rx, t.total_tx), (8_000, 4_000), "the lifetime total never resets");
    }

    #[test]
    fn period_start_handles_short_months_and_wraparound() {
        let d = |y, m, day| NaiveDate::from_ymd_opt(y, m, day).unwrap();
        // Reset on the 15th, today the 20th: the current month.
        assert_eq!(period_start(d(2026, 3, 20), 15), d(2026, 3, 15));
        // The reset day itself counts as the start of the new period.
        assert_eq!(period_start(d(2026, 3, 15), 15), d(2026, 3, 15));
        // Before the reset day the period began in the previous month.
        assert_eq!(period_start(d(2026, 3, 10), 15), d(2026, 2, 15));
        // January rolls back into the previous year.
        assert_eq!(period_start(d(2026, 1, 10), 15), d(2025, 12, 15));
        // Day 31 in February clamps to the 28th; 2028 is a leap year.
        assert_eq!(period_start(d(2026, 2, 28), 31), d(2026, 2, 28));
        assert_eq!(period_start(d(2028, 2, 29), 31), d(2028, 2, 29));
    }

    #[test]
    fn deleting_a_node_takes_its_data_with_it() {
        let db = db();
        let id = node(&db, 1);
        let probe =
            |nodes| PingTask { id: 0, name: "cm".into(), target: "1.1.1.1:443".into(), interval: 60, nodes };
        let task = db.save_ping_task(&probe(vec![id])).unwrap();
        db.accumulate(id, "b", Some((10, 10))).unwrap();
        db.insert_metric(id, 1, &serde_json::json!({"cpu": 1.0})).unwrap();
        db.insert_ping(id, task, 1, 42).unwrap();
        // 两种通知行:一条状态行,一条到期派发。
        db.transition_state_event(id, "agent_offline", 1).unwrap();
        db.record_dispatch(id, "expiry_soon", 42, 1).unwrap();
        db.delete_node(id).unwrap();
        assert!(db.node(id).unwrap().is_none());
        assert_eq!(db.metrics(id, 0, 60).unwrap().len(), 0);
        assert!(!db.all_traffic().contains_key(&id));

        // `ping_record` has no foreign key to cascade through, and SQLite reassigns
        // the deleted id to the next node created: without the sweep in
        // `delete_node` the new machine would draw the old one's chart.
        let fresh = node(&db, 1);
        assert_eq!(fresh, id, "the id is reused, which is what makes this reachable");
        db.save_ping_task(&PingTask { id: task, nodes: vec![fresh], ..probe(vec![]) }).unwrap();
        assert!(db.ping_records(fresh, 0, 60).unwrap().0.is_empty(), "and it starts with no history");

        // `notification_log` 同样没有外键,同一个复用的 id 会继承旧机器的
        // 通知历史:一条遗留的状态行读作"已经离线",新机器第一次真正掉线
        // 的告警会被它压掉。
        let log_rows = |id: i64| {
            db.conn()
                .query_row("SELECT COUNT(*) FROM notification_log WHERE node_id=?1", [id], |r| {
                    r.get::<_, i64>(0)
                })
                .unwrap()
        };
        assert_eq!(log_rows(fresh), 0, "the reused id inherits no notification history");
        assert_eq!(db.current_state_event(fresh).unwrap(), None);
    }

    /// The mirror of the sweep above, on the other key of the same table. SQLite
    /// reuses a deleted probe's id as well, and the chart selects on a node's
    /// assignments, so the removed probe's samples would reappear under the new
    /// probe's name with its timeouts folded into the new loss figure.
    #[test]
    fn deleting_a_probe_takes_its_history_with_it() {
        let db = db();
        let id = node(&db, 1);
        let probe = |name: &str| PingTask {
            id: 0,
            name: name.into(),
            target: "1.1.1.1:443".into(),
            interval: 60,
            nodes: vec![id],
        };
        let old = db.save_ping_task(&probe("tokyo")).unwrap();
        db.insert_ping(id, old, 1, 999).unwrap();
        db.delete_ping_task(old).unwrap();

        let fresh = db.save_ping_task(&probe("singapore")).unwrap();
        assert_eq!(fresh, old, "the id is reused, which is what makes this reachable");
        assert!(db.ping_records(id, 0, 60).unwrap().0.is_empty(), "and it starts with no history");
    }

    /// Counted directly from the table rather than read back through
    /// `ping_records`: that query filters on the node's assignments, so a row
    /// written under a probe it does not have is invisible to it. An assertion
    /// made through it therefore could not fail for the write this test exists to
    /// prevent.
    #[test]
    fn a_result_for_a_probe_this_node_does_not_have_is_not_stored() {
        let db = db();
        let mine = node(&db, 1);
        let other = node(&db, 1);
        let rows = || db.conn().query_row("SELECT COUNT(*) FROM ping_record", [], |r| r.get::<_, i64>(0));
        let task = db
            .save_ping_task(&PingTask {
                id: 0,
                name: "p".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes: vec![mine],
            })
            .unwrap();

        db.insert_ping(mine, task, 1, 42).unwrap();
        assert_eq!(rows().unwrap(), 1, "the node the probe is assigned to files its own result");

        // A probe that exists but belongs to another node, and ids naming no probe
        // at all: what a node token can place on the wire.
        db.insert_ping(other, task, 1, 42).unwrap();
        for invented in [7, 999_999, i64::from(i32::MAX) + 1] {
            db.insert_ping(mine, invented, 1, 42).unwrap();
        }
        assert_eq!(rows().unwrap(), 1, "nothing else reaches the table");

        // Deleting the probe also ends its node's results, so one already in flight
        // cannot land after the sweep and be inherited by the next probe to take
        // the id.
        db.delete_ping_task(task).unwrap();
        db.insert_ping(mine, task, 2, 42).unwrap();
        assert_eq!(rows().unwrap(), 0, "a late result for a deleted probe is dropped");
    }

    /// The fleet-wide scan must agree with the per-node query bucket for bucket,
    /// and must not let one node's probe leak into another's series -- the
    /// assignment filter is correlated on the outer row's `node_id`.
    #[test]
    fn the_fleet_scan_matches_the_per_node_query_and_keeps_nodes_apart() {
        let db = db();
        let a = node(&db, 1);
        let b = node(&db, 1);
        let shared = db
            .save_ping_task(&PingTask {
                id: 0,
                name: "shared".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes: vec![a, b],
            })
            .unwrap();
        let only_a = db
            .save_ping_task(&PingTask {
                id: 0,
                name: "a-only".into(),
                target: "8.8.8.8:443".into(),
                interval: 60,
                nodes: vec![a],
            })
            .unwrap();

        db.insert_ping(a, shared, 60, 40).unwrap();
        db.insert_ping(a, shared, 61, -1).unwrap(); // a timeout in the same bucket
        db.insert_ping(a, only_a, 60, 80).unwrap();
        db.insert_ping(b, shared, 60, 300).unwrap();

        let all = db.ping_records_all(0, 60).unwrap();
        assert_eq!(all.len(), 2, "both nodes with history appear, keyed by id");

        let (a_rows, a_loss) = &all[&a];
        let (b_rows, b_loss) = &all[&b];
        assert_eq!(a_rows.len(), 2, "A carries both its probes' rows");
        assert_eq!(b_rows.len(), 1, "B carries only the shared probe");

        let latency = |rows: &[serde_json::Value], task: i64| -> Option<i64> {
            rows.iter().find(|r| r["task_id"] == task).and_then(|r| r["latency"].as_i64())
        };
        assert_eq!(latency(a_rows, shared), Some(40), "A's median is its answer, not the timeout");
        assert_eq!(latency(a_rows, only_a), Some(80), "A's own-only probe is present");
        assert_eq!(latency(b_rows, shared), Some(300), "B's series is not A's");
        assert_eq!(latency(b_rows, only_a), None, "A's private probe never leaks into B");

        // Window loss: A lost one of two under `shared`; B lost nothing. Keys are
        // the probe id as a string, matching the JSON the endpoint emits.
        assert_eq!(a_loss[&shared.to_string()].as_f64(), Some(50.0), "one lost of two is 50%");
        assert_eq!(b_loss.as_object().map(|m| m.len()), Some(0), "B lost nothing");

        // The per-node query agrees, so the batch is not a second opinion.
        let (a_alone, _) = db.ping_records(a, 0, 60).unwrap();
        assert_eq!(a_alone, *a_rows, "the fleet scan matches the per-node query");

        // An empty window is an empty map, not a panic.
        assert!(db.ping_records_all(i64::MAX - 1, 60).unwrap().is_empty(), "a window with no rows is empty");
    }

    /// The strings in a `hello` come from an unvouched machine, and six of them go
    /// straight into the frame pushed to the public page every two seconds, so
    /// their length cannot be the node's to choose. `api` enforces the same bound
    /// on the one string `agent_register` accepts.
    #[test]
    fn facts_from_an_unvouched_machine_cannot_choose_their_own_length() {
        let db = db();
        let id = node(&db, 1);
        db.save_facts(id, &serde_json::json!({"os": "A".repeat(10_000), "hostname": "x\u{7}y"}), "ip", "")
            .unwrap();
        let stored = db.node(id).unwrap().unwrap();
        assert_eq!(stored.os.chars().count(), 128);
        assert_eq!(stored.hostname, "xy", "control characters break the panel's rows");
    }

    /// The observed address is its own fact, not the geo address in `ip`: the
    /// panel reads it as the fallback for a node whose own interfaces are
    /// private, so it must round-trip untouched and leave `country` alone.
    #[test]
    fn the_observed_address_is_stored_beside_the_geo_address() {
        let db = db();
        let id = node(&db, 1);
        let facts = serde_json::json!({"ipv6": "2001:db8::1", "ipv4": "192.168.1.25"});

        db.save_facts(id, &facts, "2001:db8::1", "198.51.100.7").unwrap();
        let stored = db.node(id).unwrap().unwrap();
        assert_eq!(stored.ip, "2001:db8::1", "ip 仍是地理地址");
        assert_eq!(stored.ipv4, "192.168.1.25");
        assert_eq!(stored.observed_ip, "198.51.100.7");

        // 只换观察值不清空 country:失效判断只挂在 `ip` 上。
        db.set_country(id, "US", "2001:db8::1").unwrap();
        db.save_facts(id, &facts, "2001:db8::1", "203.0.113.9").unwrap();
        assert_eq!(db.node(id).unwrap().unwrap().country, "US", "观察值变化不清空 country");

        // 空串表示这次没有可用的观察值,是允许的状态。
        db.save_facts(id, &facts, "2001:db8::1", "").unwrap();
        assert_eq!(db.node(id).unwrap().unwrap().observed_ip, "");
    }

    /// A correction must survive the node's return. `all_traffic` gates the month
    /// figures on the period they were written for, and `accumulate` restarts the
    /// counter when the stored period is stale, so a correction left under the
    /// previous period would read as zero and then be discarded.
    #[test]
    fn a_month_correction_is_stamped_with_the_period_it_was_made_in() {
        let db = db();
        let id = node(&db, 1);
        db.accumulate(id, "boot-a", Some((0, 0))).unwrap();
        // A node silent since before its reset day still holds the old period.
        db.conn().execute("UPDATE traffic SET month_start='1999-01-01' WHERE node_id=?1", [id]).unwrap();

        db.set_traffic(
            id,
            &TrafficPatch {
                total_rx: Some(4_000),
                total_tx: Some(2_000),
                month_rx: Some(300),
                month_tx: Some(100),
            },
        )
        .unwrap();
        let t = db.all_traffic().remove(&id).unwrap();
        assert_eq!((t.month_rx, t.month_tx), (300, 100), "the correction reads back as this period's");

        let t = db.accumulate(id, "boot-a", Some((500, 50))).unwrap();
        assert_eq!((t.month_rx, t.month_tx), (800, 150), "and the next report adds to it");
        assert_eq!((t.total_rx, t.total_tx), (4_500, 2_050));
    }

    #[test]
    fn partial_edits_keep_other_settings_and_live_counters() {
        let db = db();
        let id = node(&db, 1);
        let patch = |v| serde_json::from_value::<NodePatch>(v).unwrap();
        // v2 起 financial 字段(`price`/`expires_at` 等)由财务插件持有(NodePatch
        // 不再接受)。这里只覆盖宿主域的字段。注释:在曾经的 v1 测试中,这两次
        // update_node 调用原本写 `{"price":20}` 与 `{"price":0,"expires_at":null}`,
        // 触发 NodePatch 中现已删除的字段——U7 之后它们不再属于宿主 API。
        db.update_node(id, &patch(serde_json::json!({"public":false,"remark":"private"}))).unwrap();
        let n = db.node(id).unwrap().unwrap();
        assert!(!n.public);
        assert_eq!(n.remark, "private");
        db.update_node(id, &patch(serde_json::json!({"public":true}))).unwrap();

        db.accumulate(id, "boot", Some((0, 0))).unwrap();
        db.accumulate(id, "boot", Some((120_000, 10_000))).unwrap();
        db.set_traffic(id, &TrafficPatch { month_tx: Some(3_000), ..Default::default() }).unwrap();
        let t = db.all_traffic().remove(&id).unwrap();
        assert_eq!((t.total_rx, t.total_tx, t.month_rx, t.month_tx), (120_000, 10_000, 120_000, 3_000));

        db.update_node(id, &patch(serde_json::json!({"traffic_reset_day":2}))).unwrap();
        db.set_traffic(id, &TrafficPatch { month_rx: Some(7_000), ..Default::default() }).unwrap();
        let t = db.all_traffic().remove(&id).unwrap();
        assert_eq!((t.month_rx, t.month_tx), (7_000, 0));
        // Correcting only a lifetime total cannot revive the previous month's
        // bytes.
        db.conn()
            .execute("UPDATE traffic SET month_start='1999-01-01',month_tx=999 WHERE node_id=?1", [id])
            .unwrap();
        db.set_traffic(id, &TrafficPatch { total_rx: Some(130_000), ..Default::default() }).unwrap();
        assert_eq!(db.all_traffic()[&id].month_tx, 0);
    }

    #[test]
    fn a_token_is_readable_and_rotation_retires_the_old_one() {
        let db = db();
        let id = db.create_node(&Node { name: "n".into(), ..Default::default() }, "first-token").unwrap();

        // Readable, so the panel can display the install command without issuing a
        // new token.
        assert_eq!(db.node(id).unwrap().unwrap().token, "first-token");
        assert_eq!(db.node_by_token("first-token").unwrap(), Some(id));

        db.reset_token(id, "second-token").unwrap();
        assert_eq!(db.node(id).unwrap().unwrap().token, "second-token");
        assert_eq!(db.node_by_token("second-token").unwrap(), Some(id));
        assert_eq!(db.node_by_token("first-token").unwrap(), None, "the old token stops working");
    }

    #[test]
    fn nodes_can_be_reordered_atomically() {
        let db = db();
        let (a, b, c) = (node(&db, 1), node(&db, 1), node(&db, 1));
        let order = || db.nodes().unwrap().iter().map(|n| n.id).collect::<Vec<_>>();
        db.reorder_nodes(&[c, a, b]).unwrap();
        assert_eq!(order(), vec![c, a, b]);

        // Every rejected input leaves the existing order intact. The partial list
        // matters most: a stale tab would otherwise renumber around a node it never
        // saw.
        assert!(db.reorder_nodes(&[a, a, c]).is_err(), "duplicates");
        assert!(db.reorder_nodes(&[a, b]).is_err(), "a node left out");
        assert!(db.reorder_nodes(&[a, b, 9999]).is_err(), "an id that is not a node");
        assert_eq!(order(), vec![c, a, b]);
        // A node added afterwards goes to the end rather than wherever sort 0
        // places it.
        let d = node(&db, 1);
        assert_eq!(db.nodes().unwrap().iter().map(|n| n.id).collect::<Vec<_>>(), vec![c, a, b, d]);
    }

    #[test]
    fn prune_drops_history_but_never_traffic_totals() {
        let db = db();
        let id = node(&db, 1);
        db.accumulate(id, "b", Some((100, 100))).unwrap();
        db.accumulate(id, "b", Some((900, 900))).unwrap();
        let old = Utc::now().timestamp() - 40 * 86_400;
        db.insert_metric(id, old, &serde_json::json!({"cpu": 1.0})).unwrap();
        db.insert_metric(id, Utc::now().timestamp(), &serde_json::json!({"cpu": 2.0})).unwrap();

        db.prune(30).unwrap();
        assert_eq!(db.metrics(id, 0, 60).unwrap().len(), 1);
        assert_eq!(db.all_traffic()[&id].total_rx, 800);
    }

    /// The rekeying in `open()`: rows must survive it, and the chart's query must
    /// emerge able to seek. A migration that leaves every row on the old key fails
    /// silently, and stays silent while the query it exists for scans a node's
    /// entire history.
    #[test]
    fn rekeying_ping_record_keeps_the_rows_and_lets_the_chart_query_seek() {
        let file = std::env::temp_dir().join(format!("monitor-rekey-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        // A database as an older hub left it.
        let old = Connection::open(path).unwrap();
        old.execute_batch(
            "CREATE TABLE ping_record (
               node_id INTEGER NOT NULL, task_id INTEGER NOT NULL,
               ts INTEGER NOT NULL, latency INTEGER NOT NULL,
               PRIMARY KEY (node_id, task_id, ts)
             ) WITHOUT ROWID;
             INSERT INTO ping_record VALUES (1,7,100,12),(1,8,100,34),(1,7,200,56),(2,7,100,78);",
        )
        .unwrap();
        drop(old);

        let db = Db::open(path).unwrap();
        let conn = db.conn();
        let rows: Vec<(i64, i64, i64, i64)> = conn
            .prepare("SELECT node_id, task_id, ts, latency FROM ping_record ORDER BY node_id, ts, task_id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows, vec![(1, 7, 100, 12), (1, 8, 100, 34), (1, 7, 200, 56), (2, 7, 100, 78)]);

        // Without the timestamp second in the key the plan stops at `node_id=?`
        // and scans everything beneath it, and the fold in `ping_records` requires
        // rows in time order, which only the seek provides without a sorter.
        let plan: String = conn
            .prepare(&format!("EXPLAIN QUERY PLAN {PING_ROWS}"))
            .unwrap()
            .query_map(params![1, 0, 60], |r| r.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join(" | ");
        assert!(plan.contains("node_id=? AND ts>?"), "the window has to be a seek, not a scan: {plan}");
        assert!(!plan.contains("ORDER BY"), "the time order has to come off the key, not a sorter: {plan}");

        // Opening again must not rebuild a table that is already correct.
        drop(conn);
        drop(db);
        assert!(Db::open(path).is_ok());
        let _ = std::fs::remove_file(&file);
    }

    /// Dropping `metric.load1` under a database in service. The column is
    /// `NOT NULL` with no default, so a migration that silently failed to run
    /// would not merely leave a stale column: it would prevent every history row
    /// from being written.
    #[test]
    fn dropping_load1_keeps_the_history_and_lets_new_rows_in() {
        let file = std::env::temp_dir().join(format!("monitor-load1-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        // A database as a hub predating this build left it: one metric row
        // carrying a load average, stamped with the schema version of the time.
        let old = Connection::open(path).unwrap();
        old.execute_batch(
            "CREATE TABLE metric (
               node_id INTEGER NOT NULL, ts INTEGER NOT NULL,
               cpu REAL NOT NULL, load1 REAL NOT NULL,
               mem_used INTEGER NOT NULL, swap_used INTEGER NOT NULL, disk_used INTEGER NOT NULL,
               net_rx INTEGER NOT NULL, net_tx INTEGER NOT NULL,
               tcp INTEGER NOT NULL, udp INTEGER NOT NULL, procs INTEGER NOT NULL,
               PRIMARY KEY (node_id, ts)
             ) WITHOUT ROWID;
             INSERT INTO metric VALUES (1,60,12.5,0.75,100,0,0,0,0,0,0,0);
             PRAGMA user_version = 1;",
        )
        .unwrap();
        drop(old);

        let db = Db::open(path).unwrap();
        assert!(!schema_mentions(&db.conn(), "metric", "load1").unwrap(), "the column has to be gone");
        // The row remains, along with everything else it carried.
        let kept = &db.metrics(1, 0, 60).unwrap()[0];
        assert_eq!((kept["ts"].as_i64(), kept["cpu"].as_f64()), (Some(60), Some(12.5)));
        // The shape this build inserts now fits the table.
        db.insert_metric(1, 120, &serde_json::json!({"cpu": 2.0, "load": [0.5, 0.4, 0.3]})).unwrap();
        assert_eq!(db.metrics(1, 0, 60).unwrap().len(), 2);

        // Opening again must not attempt to drop a column already removed.
        drop(db);
        assert!(Db::open(path).is_ok());
        let _ = std::fs::remove_file(&file);
    }

    /// Removing a node from a probe must remove the probe from that node's chart.
    /// `ping_record` carries no foreign key to the assignment that produced it, so
    /// the rows outlive it until retention; the window query is what must stop
    /// drawing them, and immediately rather than at the next hourly sweep.
    #[test]
    fn a_probe_taken_off_a_node_stops_appearing_in_its_history() {
        let db = db();
        let id = node(&db, 1);
        let probe = |nodes: Vec<i64>, task| {
            db.save_ping_task(&PingTask {
                id: task,
                name: "cm".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes,
            })
            .unwrap()
        };
        let task = probe(vec![id], 0);
        db.insert_ping(id, task, 100, 42).unwrap();
        assert_eq!(db.ping_records(id, 0, 60).unwrap().0.len(), 1, "an assigned probe draws");

        probe(vec![], task);
        assert!(db.ping_records(id, 0, 60).unwrap().0.is_empty(), "an unassigned one does not");

        // The rows remain: reassigning restores the history rather than starting
        // over.
        probe(vec![id], task);
        assert_eq!(db.ping_records(id, 0, 60).unwrap().0.len(), 1, "and it comes back with its history");

        // The names accompany those samples and follow the same filter: a probe
        // name is operator-supplied text that routinely carries a hostname or a
        // customer.
        assert_eq!(db.ping_task_names(id).unwrap()[&task.to_string()], "cm");
        let other = node(&db, 1);
        assert!(
            db.ping_task_names(other).unwrap().as_object().is_some_and(|m| m.is_empty()),
            "a node the probe was never assigned to must not learn its name"
        );
    }

    #[test]
    fn ping_tasks_round_trip_with_their_node_assignments() {
        let db = db();
        let (a, b) = (node(&db, 1), node(&db, 1));
        let id = db
            .save_ping_task(&PingTask {
                id: 0,
                name: "cf".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes: vec![a, b],
            })
            .unwrap();
        assert_eq!(db.ping_tasks_for(a).unwrap().len(), 1);

        // Reassigning to one node must drop the other's copy.
        db.save_ping_task(&PingTask {
            id,
            name: "cf".into(),
            target: "1.1.1.1:443".into(),
            interval: 30,
            nodes: vec![a],
        })
        .unwrap();
        assert_eq!(db.ping_tasks_for(b).unwrap().len(), 0);
        assert_eq!(db.ping_tasks().unwrap()[0].interval, 30);
    }

    /// The agent caps the probe list it will run and drops the remainder with
    /// nothing but a line in its own journal. The hub knows the total, so the hub
    /// issues the refusal; otherwise the panel lists probes that never ran and
    /// charts that stay empty, with the only record on the node.
    #[test]
    fn a_node_cannot_be_given_more_probes_than_the_agent_will_run() {
        let db = db();
        let id = node(&db, 1);
        let save = |task: i64, nodes: Vec<i64>| {
            db.save_ping_task(&PingTask {
                id: task,
                name: "p".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes,
            })
        };
        for _ in 0..Db::MAX_PROBES_PER_NODE {
            save(0, vec![id]).unwrap();
        }
        assert_eq!(db.ping_tasks_for(id).unwrap().len() as i64, Db::MAX_PROBES_PER_NODE);

        let refused = save(0, vec![id]).expect_err("one past the cap must be refused");
        assert!(refused.to_string().contains("探测任务"), "{refused}");
        // Rolled back entirely: the probe must not survive its assignment being
        // rejected, or the panel accumulates one that never runs.
        assert_eq!(db.ping_tasks().unwrap().len() as i64, Db::MAX_PROBES_PER_NODE);
        assert_eq!(db.ping_tasks_for(id).unwrap().len() as i64, Db::MAX_PROBES_PER_NODE);

        // Editing an existing probe does not count as adding one.
        let first = db.ping_tasks().unwrap()[0].id;
        save(first, vec![id]).expect("an existing probe can still be edited at the cap");
    }

    /// A fresh database carries the tables this build expects, and the
    /// backup gates rely on `TABLES` naming every one of them. A fresh file
    /// also never grows the retired financial columns: `SCHEMA` no longer
    /// declares them.
    #[test]
    fn a_fresh_database_is_on_schema_v7_with_the_new_tables() {
        let db = db();
        let conn = db.conn();
        assert_eq!(conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        let table = |name: &str| {
            conn.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1", [name], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap()
        };
        assert_eq!(table("plugin"), 1);
        assert_eq!(table("notification_log"), 1);
        assert_eq!(table("plugin_data"), 1);
        let node_columns = columns_of(&conn, "node").unwrap();
        for retired in ["price", "currency", "billing_cycle", "expires_at"] {
            assert!(!node_columns.contains(retired), "新库不该带退役列 {retired}");
        }
        assert!(node_columns.contains("observed_ip"), "新库必须自己就带这一列,否则 save_facts 一写就失败");
        drop(conn);

        assert!(TABLES.contains(&"plugin"));
        assert!(TABLES.contains(&"notification_log"));
        assert!(TABLES.contains(&"plugin_data"));
    }

    /// A database left at v3 by the previous build: opening it must stamp the
    /// current version, add the three new tables, and keep the rows it already
    /// held. Opening the result again must not redo anything that cannot be
    /// redone.
    #[test]
    fn a_v3_database_upgrades_to_v7_and_reopens_cleanly() {
        let file = std::env::temp_dir().join(format!("monitor-v3-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        // A hub at v3: this build's schema minus the v4+v5 tables, carrying
        // a node that must survive the upgrade.
        let old = Connection::open(path).unwrap();
        old.execute_batch(SCHEMA).unwrap();
        old.execute_batch(
            "DROP TABLE plugin;
             DROP TABLE notification_log;
             DROP TABLE plugin_data;
             INSERT INTO node (name, token, created_at) VALUES ('kept', 't', 1);
             PRAGMA user_version = 3;",
        )
        .unwrap();
        drop(old);

        let db = Db::open(path).unwrap();
        let conn = db.conn();
        assert_eq!(conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        for table in ["plugin", "notification_log", "plugin_data"] {
            let found: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(found, 1, "{table} must exist after the upgrade");
        }
        drop(conn);
        assert_eq!(db.nodes().unwrap().len(), 1, "the node the v3 hub had survives");

        // Reopening is a no-op: the migration chain stops before the stamp.
        drop(db);
        let again = Db::open(path).unwrap();
        assert_eq!(again.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        drop(again);
        let _ = std::fs::remove_file(&file);
    }

    /// v7 adds `observed_ip` to an existing database.
    ///
    /// The fixture is a hand-written v6 `node` list, not `SCHEMA` minus some
    /// tables: `SCHEMA` already declares the new column, so a fixture derived
    /// from it would carry the column itself, `migrate_to_7` would never run,
    /// and the test would stay green with the migration deleted.
    #[test]
    fn a_v6_database_gains_the_observed_address_column() {
        let file = std::env::temp_dir().join(format!("monitor-v6-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        let old = Connection::open(path).unwrap();
        old.execute_batch(
            "CREATE TABLE node (
               id INTEGER PRIMARY KEY,
               name TEXT NOT NULL,
               token TEXT NOT NULL UNIQUE,
               sort INTEGER NOT NULL DEFAULT 0,
               public INTEGER NOT NULL DEFAULT 1,
               remark TEXT NOT NULL DEFAULT '',
               traffic_limit INTEGER NOT NULL DEFAULT 0,
               traffic_mode TEXT NOT NULL DEFAULT 'sum',
               traffic_reset_day INTEGER NOT NULL DEFAULT 1,
               hostname TEXT NOT NULL DEFAULT '', os TEXT NOT NULL DEFAULT '',
               kernel TEXT NOT NULL DEFAULT '', arch TEXT NOT NULL DEFAULT '',
               virt TEXT NOT NULL DEFAULT '', cpu_name TEXT NOT NULL DEFAULT '',
               cpu_cores INTEGER NOT NULL DEFAULT 0, mem_total INTEGER NOT NULL DEFAULT 0,
               swap_total INTEGER NOT NULL DEFAULT 0, disk_total INTEGER NOT NULL DEFAULT 0,
               agent_version TEXT NOT NULL DEFAULT '', ip TEXT NOT NULL DEFAULT '',
               ipv4 TEXT NOT NULL DEFAULT '', ipv6 TEXT NOT NULL DEFAULT '',
               country TEXT NOT NULL DEFAULT '',
               last_seen INTEGER NOT NULL DEFAULT 0,
               created_at INTEGER NOT NULL
             );
             INSERT INTO node (name, token, created_at) VALUES ('kept', 't', 1);
             PRAGMA user_version = 6;",
        )
        .unwrap();
        assert!(
            !columns_of(&old, "node").unwrap().contains("observed_ip"),
            "夹具必须真的没有这一列,否则这个测试没有判别力"
        );
        drop(old);

        let db = Db::open(path).unwrap();
        let conn = db.conn();
        assert_eq!(conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        assert!(columns_of(&conn, "node").unwrap().contains("observed_ip"), "migrate_to_7 必须加上这一列");
        drop(conn);
        assert_eq!(db.nodes().unwrap().len(), 1, "升级保留原有节点");
        // 迁移上来的库要能通过恢复校验:缺 DDL 时这一条会以 missing 拒收。
        db.check_backup(path).expect("a migrated database must pass the backup gate");
        drop(db);

        let _ = std::fs::remove_file(&file);
    }

    /// A database parked at `GATED_VERSION`: the v6 column-drop gate is still
    /// shut, so the stamp stays at 5 and `migrate_to_7` runs again on every
    /// open. The column must still arrive -- it deliberately does not sit
    /// behind that gate -- and the re-run must be tolerated.
    ///
    /// The fixture drops `observed_ip` from a `SCHEMA`-built database on
    /// purpose: `SCHEMA` declares the column, so a fixture that kept it would
    /// carry the very thing under test and prove nothing about the migration.
    #[test]
    fn a_database_parked_at_the_v6_gate_still_gains_the_observed_address_column() {
        let file = std::env::temp_dir().join(format!("monitor-v5obs-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        let old = Connection::open(path).unwrap();
        old.execute_batch(SCHEMA).unwrap();
        old.execute_batch(
            "ALTER TABLE node DROP COLUMN observed_ip;
             INSERT INTO node (name, token, created_at) VALUES ('kept', 't', 1);",
        )
        .unwrap();
        // A real v5 database still carries the four financial columns the v6
        // gate exists to drop; without them `migrate_to_6` finds nothing to do,
        // reports the gate open, and the version advances -- which is a v6
        // database, not the parked one under test.
        for column in [
            "price REAL NOT NULL DEFAULT 0",
            "currency TEXT NOT NULL DEFAULT 'USD'",
            "billing_cycle TEXT NOT NULL DEFAULT 'monthly'",
            "expires_at TEXT",
        ] {
            old.execute(&format!("ALTER TABLE node ADD COLUMN {column}"), []).unwrap();
        }
        old.execute_batch("PRAGMA user_version = 5;").unwrap();
        assert!(!columns_of(&old, "node").unwrap().contains("observed_ip"), "夹具必须真的没有这一列");
        assert!(columns_of(&old, "node").unwrap().contains("price"), "夹具必须真的是 v5 形状");
        drop(old);

        // The gate is still shut, so the stamp must not advance past it...
        let db = Db::open(path).unwrap();
        assert_eq!(db.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 5);
        // ...yet the column is there, because it is not behind that gate.
        assert!(columns_of(&db.conn(), "node").unwrap().contains("observed_ip"));
        let id = db.nodes().unwrap()[0].id;
        db.save_facts(id, &serde_json::json!({"hostname": "h"}), "8.8.8.8", "8.8.8.8").unwrap();
        assert_eq!(db.node(id).unwrap().unwrap().observed_ip, "8.8.8.8");
        drop(db);

        // Reopening re-runs the migration against a column that already exists;
        // `add_column` tolerates the duplicate and the row survives.
        let again = Db::open(path).unwrap();
        assert_eq!(again.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 5);
        assert_eq!(again.node(id).unwrap().unwrap().observed_ip, "8.8.8.8");
        drop(again);

        let _ = std::fs::remove_file(&file);
    }
    /// A database stamped newer than this binary is refused before anything
    /// touches it: an older hub would read and write tables it does not know,
    /// and would stamp its own version over the newer one.
    #[test]
    fn opening_a_newer_database_is_refused() {
        let file = std::env::temp_dir().join(format!("monitor-newer-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();
        let newer = Connection::open(path).unwrap();
        newer.execute_batch(SCHEMA).unwrap();
        newer.execute_batch(&format!("PRAGMA user_version = {}", SCHEMA_VERSION + 1)).unwrap();
        drop(newer);

        let err = match Db::open(path) {
            Ok(_) => panic!("比本二进制新的库应当被拒"),
            Err(e) => e.to_string(),
        };
        assert!(err.contains("newer hub"), "应给出可操作的提示,实际:{err}");
        // 文件停在原版本,没有被降级盖上本二进制的版本号。
        let probe = Connection::open(path).unwrap();
        let v: i64 = probe.query_row("PRAGMA user_version", [], |r| r.get(0)).unwrap();
        assert_eq!(v, SCHEMA_VERSION + 1, "被拒的库不该被改写 user_version");
        drop(probe);
        let _ = std::fs::remove_file(&file);
    }

    /// The retired financial columns are dropped in two stages (KTD10): a
    /// pre-v6 database that still carries them waits until the finance plugin
    /// has imported its nodes into `plugin_data`, so an upgrade that swaps the
    /// binary before installing the plugin keeps the columns rather than
    /// dropping them out from under data the plugin has not read yet.
    #[test]
    fn retired_node_columns_drop_only_after_the_finance_plugin_imports() {
        let file = std::env::temp_dir().join(format!("monitor-v5gate-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&file);
        let path = file.to_str().unwrap();

        // A pre-v6 hub: current schema with the four columns bolted back on and
        // no plugin data of any kind.
        let old = Connection::open(path).unwrap();
        old.execute_batch(SCHEMA).unwrap();
        for column in [
            "price REAL NOT NULL DEFAULT 0",
            "currency TEXT NOT NULL DEFAULT 'USD'",
            "billing_cycle TEXT NOT NULL DEFAULT 'monthly'",
            "expires_at TEXT",
        ] {
            old.execute(&format!("ALTER TABLE node ADD COLUMN {column}"), []).unwrap();
        }
        old.execute_batch(
            "INSERT INTO node (name, token, created_at) VALUES ('kept', 't', 1); PRAGMA user_version = 5;",
        )
        .unwrap();
        drop(old);

        // No import yet: the columns stay and the version does not advance.
        let db = Db::open(path).unwrap();
        assert_eq!(db.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 5);
        assert!(columns_of(&db.conn(), "node").unwrap().contains("price"), "导入完成前列还在");
        db.plugin_data_put("com.example.other", "unrelated", "x").unwrap();
        drop(db);

        // A plugin_data row that is not a node import does not open the gate.
        let db = Db::open(path).unwrap();
        assert_eq!(db.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 5);
        assert!(columns_of(&db.conn(), "node").unwrap().contains("price"), "非 node: 记录不开闸");
        // 别的插件用了同一个 `node:` 前缀也算不开闸:闸门读的是财务插件的私有
        // key 布局,不能由第三方插件替它触发这次不可逆删列。
        db.plugin_data_put("com.example.other", "node:1", "{}").unwrap();
        drop(db);

        let db = Db::open(path).unwrap();
        assert_eq!(db.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 5);
        assert!(
            columns_of(&db.conn(), "node").unwrap().contains("price"),
            "别的插件的 node: 记录不开闸——闸门按 plugin_id 限定"
        );
        drop(db);

        // The finance plugin imports: the next open drops the columns and lands
        // on the current version, with the node row intact.
        let db = Db::open(path).unwrap();
        db.plugin_data_put("io.github.monitor.finance-stats", "node:1", "{\"price\":0}").unwrap();
        drop(db);

        let db = Db::open(path).unwrap();
        assert_eq!(db.conn().query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        let columns = columns_of(&db.conn(), "node").unwrap();
        for retired in ["price", "currency", "billing_cycle", "expires_at"] {
            assert!(!columns.contains(retired), "导入完成后 {retired} 应已删除");
        }
        assert_eq!(db.nodes().unwrap().len(), 1, "删列保住 node 行");
        drop(db);
        let _ = std::fs::remove_file(&file);
    }

    /// A backup taken by a v3 hub carries none of the newer tables, and restore
    /// is the one path that migrates a file `Db::open` never sees:
    /// `check_backup` opens the upload on a bare connection. The migration
    /// must create the v4+v5 tables there, or every backup older than this
    /// build is refused.
    #[test]
    fn a_v3_backup_survives_check_backup() {
        let scratch = Scratch::new();
        let live = Db::open(&scratch.0).unwrap();
        node(&live, 1);

        // The upload: this build's schema minus the v4+v5 tables, stamped as
        // a v3 hub would have left it.
        let old_path = format!("{}.copy", scratch.0);
        let old = Connection::open(&old_path).unwrap();
        old.execute_batch(SCHEMA).unwrap();
        old.execute_batch(
            "DROP TABLE plugin; DROP TABLE notification_log; DROP TABLE plugin_data; PRAGMA user_version = 3;",
        )
        .unwrap();
        drop(old);

        // The candidate carries no foreign tables or rows, only the shape; it
        // must pass every gate and come out with the newer tables created.
        live.check_backup(&old_path).unwrap();
        let checked = Connection::open(&old_path).unwrap();
        assert_eq!(checked.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0)).unwrap(), 7);
        for table in ["plugin", "notification_log", "plugin_data"] {
            let found: i64 = checked
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(found, 1, "{table} must exist on the migrated upload");
        }
    }
}

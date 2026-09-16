//! 插件与通知日志的数据访问,自 db.rs 拆出。SCHEMA、迁移与备份仍归
//! db.rs 所有;这里只有读写 `plugin`、`plugin_data` 与 `notification_log`
//! 三张表的方法,以第二个 `impl Db` 块挂在同一个类型上。`PluginRow`
//! 经 db.rs 的 `pub use` 对外保持原路径可见。

use anyhow::{Context, Result};
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

use crate::db::Db;
use crate::notification_bus::Event;

/// One stored plugin: its manifest, wasm bytes and lifecycle flags.
#[derive(Serialize, Debug, Clone)]
pub struct PluginRow {
    pub id: i64,
    pub plugin_id: String,
    pub name: String,
    pub version: String,
    pub manifest_json: String,
    /// Empty in the summary view served to the panel's list: the wasm module
    /// is megabytes and the list needs none of it.
    pub wasm_blob: Vec<u8>,
    pub wasm_sha256: String,
    pub enabled: bool,
    pub status: String,
    pub last_error: Option<String>,
    pub uploaded_at: i64,
}

/// The state event on the other side of `event_type`, if it names one. A state
/// event's row and its opposite's are a pair: at most one may stand, or the
/// hub would replay "offline" while the node is already known to be offline.
fn opposite_state(event_type: &str) -> Option<&'static str> {
    match event_type {
        Event::AGENT_OFFLINE => Some(Event::AGENT_ONLINE),
        Event::AGENT_ONLINE => Some(Event::AGENT_OFFLINE),
        _ => None,
    }
}

impl Db {
    // ---- plugins ----

    /// The plugin SELECT's column list, spelled out rather than `SELECT *` so
    /// the summary view below can substitute its one lighter column. The two
    /// constants must stay in step with `row_to_plugin`, which reads by name.
    const PLUGIN_COLUMNS: &str = "id, plugin_id, name, version, manifest_json, wasm_blob,
                    wasm_sha256, enabled, status, last_error, uploaded_at";
    /// [`Self::PLUGIN_COLUMNS`] with the megabyte blob replaced by an empty one:
    /// the panel's list needs none of the module's bytes.
    const PLUGIN_COLUMNS_SUMMARY: &str = "id, plugin_id, name, version, manifest_json, x'' AS wasm_blob,
                    wasm_sha256, enabled, status, last_error, uploaded_at";

    /// Stores an uploaded plugin and returns the row as it now stands. A
    /// `plugin_id` collision surfaces as an error the caller turns into a 400
    /// rather than a silent clobber of the previous upload.
    pub fn create_plugin(
        &self,
        plugin_id: &str,
        name: &str,
        version: &str,
        manifest_json: &str,
        wasm_blob: &[u8],
        wasm_sha256: &str,
    ) -> Result<PluginRow> {
        let now = Utc::now().timestamp();
        let conn = self.conn();
        conn.execute(
            "INSERT INTO plugin (plugin_id, name, version, manifest_json, wasm_blob,
                                 wasm_sha256, enabled, status, uploaded_at)
             VALUES (?1,?2,?3,?4,?5,?6,0,'disabled',?7)",
            params![plugin_id, name, version, manifest_json, wasm_blob, wasm_sha256, now],
        )
        .with_context(|| format!("plugin {plugin_id} is already uploaded"))?;
        let id = conn.last_insert_rowid();
        Ok(PluginRow {
            id,
            plugin_id: plugin_id.into(),
            name: name.into(),
            version: version.into(),
            manifest_json: manifest_json.into(),
            wasm_blob: wasm_blob.into(),
            wasm_sha256: wasm_sha256.into(),
            enabled: false,
            status: "disabled".into(),
            last_error: None,
            uploaded_at: now,
        })
    }

    pub fn list_plugins(&self) -> Result<Vec<PluginRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM plugin ORDER BY uploaded_at DESC, id DESC",
            Self::PLUGIN_COLUMNS
        ))?;
        let rows = stmt.query_map([], row_to_plugin)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Just the `plugin_id` behind a row, or `None` when there is no such row.
    /// For callers that need only the existence or the identifier: reading the
    /// whole row would drag the wasm blob along for nothing.
    pub fn plugin_id_of(&self, id: i64) -> Result<Option<String>> {
        Ok(self
            .conn()
            .query_row("SELECT plugin_id FROM plugin WHERE id=?1", [id], |r| r.get(0))
            .optional()?)
    }

    /// Every enabled plugin, for the registry's startup preload: a disabled
    /// plugin's blob is never compiled, so it is not read either.
    pub fn enabled_plugins(&self) -> Result<Vec<PluginRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM plugin WHERE enabled=1 ORDER BY uploaded_at DESC, id DESC",
            Self::PLUGIN_COLUMNS
        ))?;
        let rows = stmt.query_map([], row_to_plugin)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    pub fn get_plugin(&self, id: i64) -> Result<Option<PluginRow>> {
        Ok(self
            .conn()
            .query_row(
                &format!("SELECT {} FROM plugin WHERE id=?1", Self::PLUGIN_COLUMNS),
                [id],
                row_to_plugin,
            )
            .optional()?)
    }

    /// The panel's list. The wasm bytes are megabytes per plugin and the list
    /// needs none of them, so the column is left out of the query rather than
    /// read and thrown away.
    pub fn plugin_summaries(&self) -> Result<Vec<PluginRow>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(&format!(
            "SELECT {} FROM plugin ORDER BY uploaded_at DESC, id DESC",
            Self::PLUGIN_COLUMNS_SUMMARY
        ))?;
        let rows = stmt.query_map([], row_to_plugin)?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Enabling and disabling are the same switch: `status` mirrors `enabled`
    /// so a reader that only looks at one of them cannot be lied to, and a
    /// fresh enable starts from no error.
    pub fn set_plugin_enabled(&self, id: i64, enabled: bool) -> Result<()> {
        let status = if enabled { "enabled" } else { "disabled" };
        self.conn().execute(
            "UPDATE plugin SET enabled=?2, status=?3, last_error=NULL WHERE id=?1",
            params![id, enabled, status],
        )?;
        Ok(())
    }

    /// Records the loader's verdict on a plugin: running, disabled, or failed
    /// with the error that stopped it.
    pub fn set_plugin_status(&self, id: i64, status: &str, last_error: Option<&str>) -> Result<()> {
        self.conn().execute(
            "UPDATE plugin SET status=?2, last_error=?3 WHERE id=?1",
            params![id, status, last_error],
        )?;
        Ok(())
    }

    /// Bails on an id that matches nothing, so a delete routed to a removed
    /// plugin surfaces as an error the caller turns into a 404 rather than a
    /// success that changed nothing.
    pub fn delete_plugin(&self, id: i64) -> Result<()> {
        let gone = self.conn().execute("DELETE FROM plugin WHERE id=?1", [id])?;
        if gone == 0 {
            anyhow::bail!("no plugin {id}");
        }
        Ok(())
    }

    /// 删除插件的行、它的全部 kv 行与 plugin_data 行,一条事务里三条 DELETE。
    /// 此前是两次独立调用:行删成功、kv 清理失败时调用方拿到 500,而重试在
    /// api 的 plugin_or_404 门上变成 404,kv 孤儿从此永久留在 setting 表里。
    /// 行删失败(行已不在)整体回滚并报错,与 [`Db::delete_plugin`] 一致。
    /// kv 的模式与转义理由见 [`Db::plugin_kv`]。
    pub fn delete_plugin_with_kv(&self, id: i64, plugin_id: &str) -> Result<()> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let gone = tx.execute("DELETE FROM plugin WHERE id=?1", [id])?;
        if gone == 0 {
            anyhow::bail!("no plugin {id}");
        }
        tx.execute(
            "DELETE FROM setting WHERE key LIKE ?1 ESCAPE '\\'",
            [format!("plugin.{}:%", like_escaped(plugin_id))],
        )?;
        tx.execute("DELETE FROM plugin_data WHERE plugin_id=?1", [plugin_id])?;
        tx.commit()?;
        Ok(())
    }

    /// 一个插件的全部 kv 行,`plugin.<plugin_id>:` 前缀,去掉前缀后的 key 与
    /// 值成对返回,按 key 排序让面板的列表稳定。U5 的 KV 面板与删除清理用。
    ///
    /// 前缀匹配走 LIKE,而 plugin_id 只禁 `:` 不禁 `_` 与 `%`——它们在 LIKE 里
    /// 是通配符,一个 `com.example_tg` 的前缀会匹配到 `com.exampleXtg` 的行,所以
    /// 调用方传入的 plugin_id 必须经 [`like_escaped`] 转义后才能拼进模式。
    pub fn plugin_kv(&self, plugin_id: &str) -> Result<Vec<(String, String)>> {
        let prefix = format!("plugin.{plugin_id}:");
        let pattern = format!("{}%", like_escaped(&prefix));
        let conn = self.conn();
        let mut stmt =
            conn.prepare("SELECT key, value FROM setting WHERE key LIKE ?1 ESCAPE '\\' ORDER BY key")?;
        let rows = stmt.query_map([pattern], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
        let pairs = rows
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            // 前缀里的 `%` 已转义,剥离是纯字符串操作,长度必然吻合。
            .map(|(key, value)| (key[prefix.len()..].to_owned(), value))
            .collect();
        Ok(pairs)
    }

    /// 删除一个插件的全部 kv 行(删除插件时调用),返回删掉的行数。
    /// 模式与转义的理由见 [`Db::plugin_kv`]。
    pub fn delete_plugin_kv(&self, plugin_id: &str) -> Result<usize> {
        let pattern = format!("plugin.{}:%", like_escaped(plugin_id));
        let gone = self.conn().execute("DELETE FROM setting WHERE key LIKE ?1 ESCAPE '\\'", [pattern])?;
        Ok(gone)
    }

    // ---- plugin_data(U2/KTD3)----
    //
    // 通用插件数据存储:插件对自己命名空间的记录集有完整 CRUD。所有方法按
    // (plugin_id, record_key) 精确寻址,插件 A 无法触及插件 B 的行(R2)。
    // 记录值上限与单插件总配额在宿主函数层检查(host.rs),这里只做数据访问。

    /// 插入或覆盖一行记录(upsert)。返回是否新建(而非覆盖)。
    pub fn plugin_data_put(&self, plugin_id: &str, key: &str, data: &str) -> Result<bool> {
        let existed = self.conn().query_row(
            "SELECT COUNT(*) FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
            params![plugin_id, key],
            |r| r.get::<_, i64>(0),
        )? > 0;
        self.conn().execute(
            "INSERT INTO plugin_data (plugin_id, record_key, data, updated_at) VALUES (?1,?2,?3,?4)
             ON CONFLICT(plugin_id, record_key) DO UPDATE SET data=?3, updated_at=?4",
            params![plugin_id, key, data, Utc::now().timestamp()],
        )?;
        Ok(!existed)
    }

    /// 读一行记录,不存在返回 None。
    pub fn plugin_data_get(&self, plugin_id: &str, key: &str) -> Result<Option<String>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT data FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
                params![plugin_id, key],
                |r| r.get::<_, String>(0),
            )
            .optional()?)
    }

    /// 删除一行记录。返回是否确实删了一行。
    pub fn plugin_data_delete(&self, plugin_id: &str, key: &str) -> Result<bool> {
        let gone = self.conn().execute(
            "DELETE FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
            params![plugin_id, key],
        )?;
        Ok(gone > 0)
    }

    /// 一个插件按前缀匹配的全部记录,按 key 排序。空前缀列出全部。
    pub fn plugin_data_list(&self, plugin_id: &str, prefix: &str) -> Result<Vec<(String, String)>> {
        let pattern = format!("{}%", like_escaped(prefix));
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT record_key, data FROM plugin_data
             WHERE plugin_id=?1 AND record_key LIKE ?2 ESCAPE '\\' ORDER BY record_key",
        )?;
        let rows = stmt.query_map(params![plugin_id, pattern], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// 一个插件的记录数与总字节数(面板空间占用展示用,R11)。
    pub fn plugin_data_usage(&self, plugin_id: &str) -> Result<(i64, i64)> {
        Ok(self.conn().query_row(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(data)),0) FROM plugin_data WHERE plugin_id=?1",
            params![plugin_id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )?)
    }

    /// 全部插件的记录数与总字节数,按 plugin_id 聚合(R11 数据页)。
    pub fn plugin_data_usage_all(&self) -> Result<Vec<(String, i64, i64)>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(
            "SELECT plugin_id, COUNT(*), COALESCE(SUM(LENGTH(data)),0)
             FROM plugin_data GROUP BY plugin_id ORDER BY plugin_id",
        )?;
        let rows =
            stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?, r.get::<_, i64>(2)?)))?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// 删除一个插件的全部记录(删除插件时调用),返回删掉的行数。
    pub fn delete_plugin_data(&self, plugin_id: &str) -> Result<usize> {
        let gone = self.conn().execute("DELETE FROM plugin_data WHERE plugin_id=?1", params![plugin_id])?;
        Ok(gone)
    }

    /// 删除一行 setting。面板删除插件的单个 kv 行用;键名是调用方拼好的
    /// 精确键,无需 LIKE。删除不存在的行不是错误:两处面板同时打开,后点
    /// 的那个同样达成目标(与 `drop_session` 对同一竞态的处理一致)。
    pub fn delete_setting(&self, key: &str) -> Result<()> {
        self.conn().execute("DELETE FROM setting WHERE key=?1", [key])?;
        Ok(())
    }

    // ---- notification log ----

    /// True once this dispatch has been recorded. The ExpirySoon idempotency
    /// check: `key` encodes the threshold tier and the expiry date, so one
    /// alert per node, per tier, per billing cycle.
    pub fn dispatch_already_sent(&self, node_id: i64, event_type: &str, key: i64) -> Result<bool> {
        let conn = self.conn();
        let sent: i64 = conn.query_row(
            "SELECT EXISTS (SELECT 1 FROM notification_log
                            WHERE node_id=?1 AND event_type=?2 AND threshold_or_state_key=?3)",
            params![node_id, event_type, key],
            |r| r.get(0),
        )?;
        Ok(sent != 0)
    }

    /// Records an ExpirySoon dispatch. `INSERT OR IGNORE` rather than a plain
    /// insert: the check and the write are not one statement, and a dispatch
    /// racing itself must land as one row. Returns false when the row already
    /// stood, so the caller knows it was the duplicate.
    pub fn record_dispatch(&self, node_id: i64, event_type: &str, key: i64, sent_at: i64) -> Result<bool> {
        let inserted = self.conn().execute(
            "INSERT OR IGNORE INTO notification_log
               (node_id, event_type, threshold_or_state_key, sent_at, success, detail)
             VALUES (?1,?2,?3,?4,0,'')",
            params![node_id, event_type, key, sent_at],
        )?;
        Ok(inserted != 0)
    }

    /// The node's most recent state event, whichever side it was. A node with
    /// no row has never been reported offline (or the row was cleared by the
    /// transition to the other side).
    pub fn current_state_event(&self, node_id: i64) -> Result<Option<(String, i64)>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT event_type, sent_at FROM notification_log
                  WHERE node_id=?1 AND event_type IN ('agent_offline','agent_online')
                  ORDER BY sent_at DESC LIMIT 1",
                [node_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }

    /// Flips the node to `event_type` and clears the opposite side's row, both
    /// or neither. Returns false without writing when the node is already in
    /// that state: an offline node flapping its connection must not re-alert.
    pub fn transition_state_event(&self, node_id: i64, event_type: &str, sent_at: i64) -> Result<bool> {
        if self.current_state_event(node_id)?.is_some_and(|(current, _)| current == event_type) {
            return Ok(false);
        }
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        // State rows sit on key 0; the key distinguishes them from ExpirySoon
        // dispatches, which carry their tier and expiry date.
        tx.execute(
            "INSERT OR REPLACE INTO notification_log
               (node_id, event_type, threshold_or_state_key, sent_at, success, detail)
             VALUES (?1,?2,0,?3,0,'')",
            params![node_id, event_type, sent_at],
        )?;
        if let Some(other) = opposite_state(event_type) {
            tx.execute(
                "DELETE FROM notification_log
                  WHERE node_id=?1 AND event_type=?2 AND threshold_or_state_key=0",
                params![node_id, other],
            )?;
        }
        tx.commit()?;
        Ok(true)
    }

    /// Writes the dispatch outcome back: `success` and whatever the plugin
    /// said, so the panel can show what was sent and what failed.
    pub fn mark_dispatch_result(
        &self,
        node_id: i64,
        event_type: &str,
        key: i64,
        success: bool,
        detail: &str,
    ) -> Result<()> {
        self.conn().execute(
            "UPDATE notification_log SET success=?4, detail=?5
              WHERE node_id=?1 AND event_type=?2 AND threshold_or_state_key=?3",
            params![node_id, event_type, key, success, detail],
        )?;
        Ok(())
    }

    /// One dispatch row's outcome: `(success, detail)`. None when no such row
    /// stands. The read side of `mark_dispatch_result`, for the panel's log view
    /// and for the dispatch loop's tests.
    pub fn notification_log_row(
        &self,
        node_id: i64,
        event_type: &str,
        key: i64,
    ) -> Result<Option<(bool, String)>> {
        Ok(self
            .conn()
            .query_row(
                "SELECT success, detail FROM notification_log
                  WHERE node_id=?1 AND event_type=?2 AND threshold_or_state_key=?3",
                params![node_id, event_type, key],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?)
    }
}

/// 转义一个要拼进 LIKE 模式的字符串:`%` 与 `_` 是通配符,`\` 是转义符本身。
/// 配合 `ESCAPE '\'` 使用。plugin_id 允许 `_`(如 `com.example_tg`),不转义时
/// `plugin.<id>:%` 会匹配到别的插件(`com.exampleXtg`)的 kv 行。
pub(crate) fn like_escaped(s: &str) -> String {
    s.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// Column order matches every plugin SELECT, which spell out their columns
/// rather than relying on `SELECT *`: the summary view substitutes an empty
/// blob for the column it leaves out.
fn row_to_plugin(r: &rusqlite::Row<'_>) -> rusqlite::Result<PluginRow> {
    Ok(PluginRow {
        id: r.get("id")?,
        plugin_id: r.get("plugin_id")?,
        name: r.get("name")?,
        version: r.get("version")?,
        manifest_json: r.get("manifest_json")?,
        wasm_blob: r.get("wasm_blob")?,
        wasm_sha256: r.get("wasm_sha256")?,
        enabled: r.get::<_, i64>("enabled")? != 0,
        status: r.get("status")?,
        last_error: r.get("last_error")?,
        uploaded_at: r.get("uploaded_at")?,
    })
}

#[cfg(test)]
mod tests {
    use crate::db::db;

    /// The plugin round trip: upload, read back with and without the bytes,
    /// flip the lifecycle flags, and refuse a second copy of the same
    /// `plugin_id` rather than overwriting the first.
    #[test]
    fn plugins_round_trip_and_a_duplicate_plugin_id_is_refused() {
        let db = db();
        let wasm = b"\0asm-fake-module".to_vec();
        let created =
            db.create_plugin("mailer", "Mailer", "1.0.0", "{\"entry\":\"send\"}", &wasm, "sha").unwrap();
        assert_eq!(
            (created.id, created.plugin_id.as_str(), created.status.as_str()),
            (1, "mailer", "disabled")
        );
        assert!(!created.enabled);

        let back = db.get_plugin(created.id).unwrap().unwrap();
        assert_eq!(back.wasm_blob, wasm, "the stored module comes back whole");
        assert_eq!((back.name.as_str(), back.version.as_str()), ("Mailer", "1.0.0"));

        let duplicate = db
            .create_plugin("mailer", "Mailer", "2.0.0", "{}", &wasm, "sha2")
            .expect_err("a second upload of the same plugin_id must be refused");
        assert!(duplicate.to_string().contains("already uploaded"), "{duplicate}");
        assert!(db.get_plugin(created.id).unwrap().unwrap().version == "1.0.0", "the first upload stands");

        let second = db.create_plugin("webhook", "Webhook", "0.1", "{}", b"m2", "sha3").unwrap();
        // Newest upload first.
        let listed = db.list_plugins().unwrap();
        assert_eq!(
            listed.iter().map(|p| p.plugin_id.as_str()).collect::<Vec<_>>(),
            vec!["webhook", "mailer"]
        );

        let summaries = db.plugin_summaries().unwrap();
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().all(|p| p.wasm_blob.is_empty()), "the list never carries the bytes");
        assert_eq!(summaries[1].id, created.id, "everything but the blob is the same row");

        db.set_plugin_enabled(created.id, true).unwrap();
        let enabled = db.get_plugin(created.id).unwrap().unwrap();
        assert!(enabled.enabled && enabled.status == "enabled" && enabled.last_error.is_none());

        db.set_plugin_status(created.id, "error", Some("wasm would not start")).unwrap();
        assert_eq!(
            db.get_plugin(created.id).unwrap().unwrap().last_error.as_deref(),
            Some("wasm would not start")
        );

        db.delete_plugin(second.id).unwrap();
        assert!(db.get_plugin(second.id).unwrap().is_none());
        assert!(db.delete_plugin(second.id).is_err(), "deleting a removed plugin must not report success");
    }

    /// 删除插件是行与 kv 行一条事务:两端一起消失,行不在时整体报错而不
    /// 是留下半删状态。前缀的 LIKE 转义由 [`Db::plugin_kv`] 的测试覆盖,
    /// 这里只证两条 DELETE 在一个事务里。
    #[test]
    fn deleting_a_plugin_takes_its_kv_rows_in_the_same_transaction() {
        let db = db();
        let row = db.create_plugin("mailer", "Mailer", "1.0.0", "{}", b"m", "sha").unwrap();
        db.set("plugin.mailer:token", "x").unwrap();
        db.set("plugin.mailer:webhook", "y").unwrap();
        db.set("plugin.other:token", "kept").unwrap();

        db.delete_plugin_with_kv(row.id, "mailer").unwrap();
        assert!(db.get_plugin(row.id).unwrap().is_none(), "行删了");
        assert_eq!(db.get("plugin.mailer:token"), None, "kv 行随插件一起删");
        assert_eq!(db.get("plugin.mailer:webhook"), None);
        assert_eq!(db.get("plugin.other:token").as_deref(), Some("kept"), "别的插件的行不动");

        assert!(
            db.delete_plugin_with_kv(row.id, "mailer").is_err(),
            "deleting a removed plugin must not report success"
        );
    }

    /// kv 的前缀匹配必须按字符比较,而不是按 LIKE 的通配符:`_` 与 `%` 在
    /// plugin_id 里合法,不转义时一个插件的删除会吃掉另一个插件的行。
    #[test]
    fn plugin_kv_prefixes_match_the_plugin_id_not_like_wildcards() {
        let db = db();
        // 三个 `a?b`:一个下划线(合法 plugin_id)、一个点(前缀碰撞的另一半)、
        // 一个百分号。外加一个名字以 `a_b` 开头但更长的插件。
        for (key, value) in [
            ("plugin.a_b:token", "underscore"),
            ("plugin.a.b:token", "dot"),
            ("plugin.a%b:token", "percent"),
            ("plugin.aXb:token", "wildcard-victim"),
            ("plugin.a_bee:token", "longer-name"),
        ] {
            db.set(key, value).unwrap();
        }

        let kv = db.plugin_kv("a_b").unwrap();
        assert_eq!(kv, vec![("token".into(), "underscore".into())], "`_` 不能当通配符用");
        let kv = db.plugin_kv("a.b").unwrap();
        assert_eq!(kv, vec![("token".into(), "dot".into())], "点号前缀不能吃进带后缀的名字");

        // 删除同样只碰自己的命名空间。
        assert_eq!(db.delete_plugin_kv("a_b").unwrap(), 1);
        assert_eq!(db.get("plugin.a_b:token"), None);
        assert_eq!(
            db.get("plugin.aXb:token").as_deref(),
            Some("wildcard-victim"),
            "`a_b` 的删除不得匹配 `aXb`"
        );
        assert_eq!(db.get("plugin.a_bee:token").as_deref(), Some("longer-name"), "也不得匹配 `a_bee`");
        // `%` 同理:`a%b` 的模式不匹配 `aXb`。
        assert_eq!(db.delete_plugin_kv("a%b").unwrap(), 1);
        assert_eq!(db.get("plugin.aXb:token").as_deref(), Some("wildcard-victim"));
    }

    /// The ExpirySoon idempotency key in action: the first record wins, the
    /// second is told it lost, and the result lands on the row that stands.
    #[test]
    fn an_expiry_dispatch_is_recorded_once_and_its_result_written_back() {
        let db = db();
        assert!(!db.dispatch_already_sent(7, "expiry_soon", 42).unwrap(), "nothing sent, nothing recorded");

        assert!(db.record_dispatch(7, "expiry_soon", 42, 100).unwrap(), "the first dispatch records");
        assert!(db.dispatch_already_sent(7, "expiry_soon", 42).unwrap(), "and is remembered");
        assert!(!db.record_dispatch(7, "expiry_soon", 42, 200).unwrap(), "a repeat is the duplicate");
        // A different tier, or a different cycle's key, is its own dispatch.
        assert!(!db.dispatch_already_sent(7, "expiry_soon", 43).unwrap());
        assert!(!db.dispatch_already_sent(8, "expiry_soon", 42).unwrap());

        db.mark_dispatch_result(7, "expiry_soon", 42, true, "sent to 2 channels").unwrap();
        let conn = db.conn();
        let (success, detail): (i64, String) = conn
            .query_row(
                "SELECT success, detail FROM notification_log
                  WHERE node_id=7 AND event_type='expiry_soon' AND threshold_or_state_key=42",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((success, detail.as_str()), (1, "sent to 2 channels"));
    }

    /// State transitions are edges, not events: the same state twice is one
    /// row and one alert, and flipping back and forth works because the
    /// opposite side's row is what a flip clears.
    #[test]
    fn state_events_transition_once_per_side() {
        let db = db();
        assert_eq!(db.current_state_event(5).unwrap(), None, "never reported, never recorded");

        assert!(
            db.transition_state_event(5, "agent_offline", 100).unwrap(),
            "the first offline is a transition"
        );
        assert!(!db.transition_state_event(5, "agent_offline", 200).unwrap(), "a repeat is not");
        assert_eq!(db.current_state_event(5).unwrap(), Some(("agent_offline".into(), 100)));

        assert!(db.transition_state_event(5, "agent_online", 300).unwrap(), "coming back is");
        assert_eq!(db.current_state_event(5).unwrap(), Some(("agent_online".into(), 300)));
        let offline_rows: i64 = db
            .conn()
            .query_row(
                "SELECT COUNT(*) FROM notification_log WHERE node_id=5 AND event_type='agent_offline'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(offline_rows, 0, "the offline row died with the transition");

        assert!(db.transition_state_event(5, "agent_offline", 400).unwrap(), "going offline again re-alerts");
    }
}

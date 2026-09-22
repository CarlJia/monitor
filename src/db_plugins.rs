//! 插件与通知日志的数据访问,自 db.rs 拆出。SCHEMA、迁移与备份仍归
//! db.rs 所有;这里只有读写 `plugin`、`plugin_data` 与 `notification_log`
//! 三张表的方法,以第二个 `impl Db` 块挂在同一个类型上。`PluginRow`
//! 经 db.rs 的 `pub use` 对外保持原路径可见。

use anyhow::{Context, Result};
use chrono::Utc;
use monitor_plugin_contract::constants::{KV_VALUE_MAX, PLUGIN_DATA_MAX, RECORD_MAX};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use std::cmp::Ordering;

use crate::db::Db;
use crate::notification_bus::Event;
use crate::plugin::{is_newer_version, version_ordering};

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

/// 单事务快照读出的一个插件连同它的数据（U1/KTD2）：插件行全列、全部
/// `plugin_data` 记录、全部渠道 kv（已剥 `plugin.<id>:` 前缀）。导出 API
/// 把它序列化进包，导入侧再按同样的三份合并回库。
pub struct PluginExport {
    pub plugin: PluginRow,
    /// `(record_key, data)`，按 key 排序。
    pub records: Vec<(String, String)>,
    /// `(key, value)`，前缀已剥，按 key 排序。
    pub kv: Vec<(String, String)>,
}

/// 一次含数据导入的合并结果（U1/KTD4）：给面板 toast 报数用。`replaced`
/// 与纯包升级同义——true 表示覆盖/升级了已装的同款，false 表示新装。
#[derive(Debug)]
pub struct DataMergeOutcome {
    pub row: PluginRow,
    pub replaced: bool,
    pub records_merged: usize,
    pub kv_merged: usize,
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

    /// 直接建一行插件（测试与工具用）。`plugin_id` 已被占用时报错而不是覆盖：
    /// 上传路径走 [`Self::install_plugin_package`]，那里才按版本决定要不要替换。
    pub fn create_plugin(
        &self,
        plugin_id: &str,
        name: &str,
        version: &str,
        manifest_json: &str,
        wasm_blob: &[u8],
        wasm_sha256: &str,
    ) -> Result<PluginRow> {
        let conn = self.conn();
        insert_plugin_row(&conn, plugin_id, name, version, manifest_json, wasm_blob, wasm_sha256)
            .with_context(|| format!("plugin {plugin_id} is already uploaded"))
    }

    /// 安装上传的包：同 `plugin_id` 已装过时，**只有版本更高**才就地替换。
    ///
    /// 替换只换 manifest、模块与版本，行 id、kv 与 `plugin_data` 一概不动——
    /// 插件的数据按 plugin_id 存在别处，换一个包不该顺手清空它（finance-stats
    /// 的全部财务记录就在那儿，而面板上的删除会连数据一起删）。生命周期拨回
    /// 「已停用」：新包要操作员重新启用才会装载，与首次上传同一条路（KTD10），
    /// 也就不会留着一份跑在内存里的旧模块。
    ///
    /// 版本没提高就报错而不是替换：改高 `plugin.toml` 的 version 是明确的一步，
    /// 比让一个手滑的（或忘了改版本号的）包盖掉线上那份好查。读版本与写包在
    /// 同一事务里，两个并发上传不会都通过版本检查。
    ///
    /// 返回 `(行, 是否替换)`：面板据此把提示从「已上传」换成「已更新到 v…」。
    pub fn install_plugin_package(
        &self,
        plugin_id: &str,
        name: &str,
        version: &str,
        manifest_json: &str,
        wasm_blob: &[u8],
        wasm_sha256: &str,
    ) -> Result<(PluginRow, bool)> {
        let now = Utc::now().timestamp();
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let installed: Option<(i64, String)> = tx
            .query_row("SELECT id, version FROM plugin WHERE plugin_id=?1", [plugin_id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;

        let Some((id, installed_version)) = installed else {
            let row =
                insert_plugin_row(&tx, plugin_id, name, version, manifest_json, wasm_blob, wasm_sha256)?;
            tx.commit()?;
            return Ok((row, false));
        };

        if !is_newer_version(version, &installed_version) {
            anyhow::bail!(
                "插件 {plugin_id} 已装版本 {installed_version}，这次上传的是 {version}；\
                 同一 plugin_id 只有版本更高才能替换：把 plugin.toml 的 version 改高后重新打包"
            );
        }
        tx.execute(
            "UPDATE plugin SET name=?2, version=?3, manifest_json=?4, wasm_blob=?5,
                    wasm_sha256=?6, enabled=0, status='disabled', last_error=NULL, uploaded_at=?7
             WHERE id=?1",
            params![id, name, version, manifest_json, wasm_blob, wasm_sha256, now],
        )?;
        let row = tx.query_row(
            &format!("SELECT {} FROM plugin WHERE id=?1", Self::PLUGIN_COLUMNS),
            [id],
            row_to_plugin,
        )?;
        tx.commit()?;
        Ok((row, true))
    }

    /// 单事务快照读一个插件连同它的数据（U1/KTD2）：插件行全列、全部
    /// `plugin_data` 记录、全部渠道 kv 三处在**同一条事务**里读，导出期间
    /// 另一处的插件写入不会让包里三份互相撕裂。行不存在返回 `Ok(None)`，
    /// 让导出 API 转成 404。kv 的前缀匹配与转义理由见 [`Db::plugin_kv`]。
    pub fn export_plugin(&self, id: i64) -> Result<Option<PluginExport>> {
        let mut conn = self.conn();
        let tx = conn.transaction()?;
        let plugin = tx
            .query_row(
                &format!("SELECT {} FROM plugin WHERE id=?1", Self::PLUGIN_COLUMNS),
                [id],
                row_to_plugin,
            )
            .optional()?;
        let Some(plugin) = plugin else {
            return Ok(None);
        };

        let records = {
            let mut stmt = tx
                .prepare("SELECT record_key, data FROM plugin_data WHERE plugin_id=?1 ORDER BY record_key")?;
            let rows = stmt
                .query_map([&plugin.plugin_id], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
        };

        let prefix = format!("plugin.{}:", plugin.plugin_id);
        let kv = {
            let pattern = format!("{}%", like_escaped(&prefix));
            let mut stmt =
                tx.prepare("SELECT key, value FROM setting WHERE key LIKE ?1 ESCAPE '\\' ORDER BY key")?;
            let rows = stmt.query_map([pattern], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
            rows.collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .map(|(key, value)| (key[prefix.len()..].to_owned(), value))
                .collect()
        };
        // 只读，无写入，drop 即回滚（读锁释放）——无需 commit。
        Ok(Some(PluginExport { plugin, records, kv }))
    }

    /// 含数据包的导入合并（U1/KTD3、KTD4）：版本判定含**同版本旁路**、逐条
    /// 校验、合并后并集配额预检、插件行 upsert、kv 与记录合并，全部在**同一
    /// 条事务**里——任何一步失败整体回滚，一行不写（R9）。
    ///
    /// 与 [`Db::install_plugin_package`] 的差别只在版本门：纯包只有更高才替换，
    /// 含数据包把「等于（恢复）」与「更高（升级）」都放行、只拒「更低」。
    ///
    /// 合并语义是**包优先**（R8）：包内有的键恢复为包里的值，包里没有的键
    /// 保留库里现值，不删除库内任何数据；同一包重复导入结果幂等。
    #[allow(clippy::too_many_arguments)]
    pub fn install_plugin_package_with_data(
        &self,
        plugin_id: &str,
        name: &str,
        version: &str,
        manifest_json: &str,
        wasm_blob: &[u8],
        wasm_sha256: &str,
        records: &[(String, String)],
        kv: &[(String, String)],
    ) -> Result<DataMergeOutcome> {
        // 逐条形态校验先于任何库写入：坏包在开事务前就被拒（R9）。记录 key
        // 允许含 `:`（宿主写侧本就允许 `node:1` 这类），只禁空；kv key 只禁空
        // （「≤128 且无 :」是 manifest 声明层的约束，不作导入拒绝条件）。
        for (key, data) in records {
            if key.is_empty() {
                anyhow::bail!("数据记录的 key 不能为空");
            }
            if data.len() > RECORD_MAX {
                anyhow::bail!("数据记录 {key} 超过单条 {} KiB 的上限", RECORD_MAX / 1024);
            }
        }
        for (key, value) in kv {
            if key.is_empty() {
                anyhow::bail!("渠道配置的 key 不能为空");
            }
            if value.len() > KV_VALUE_MAX {
                anyhow::bail!("渠道配置 {key} 超过单项 {} KiB 的上限", KV_VALUE_MAX / 1024);
            }
        }

        let now = Utc::now().timestamp();
        let mut conn = self.conn();
        let tx = conn.transaction()?;

        let installed: Option<(i64, String)> = tx
            .query_row("SELECT id, version FROM plugin WHERE plugin_id=?1", [plugin_id], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .optional()?;

        let (id, replaced) = match &installed {
            None => {
                let row =
                    insert_plugin_row(&tx, plugin_id, name, version, manifest_json, wasm_blob, wasm_sha256)?;
                (row.id, false)
            }
            Some((id, installed_version)) => {
                // 同版本旁路：等于（恢复）或更高（升级）放行，只拒更低。
                if version_ordering(version, installed_version) == Ordering::Less {
                    anyhow::bail!(
                        "插件 {plugin_id} 已装版本 {installed_version}，这次导入的是 {version}；\
                         含数据包只有版本相等（恢复）或更高（升级）才能覆盖，更低会拒绝"
                    );
                }
                tx.execute(
                    "UPDATE plugin SET name=?2, version=?3, manifest_json=?4, wasm_blob=?5,
                            wasm_sha256=?6, enabled=0, status='disabled', last_error=NULL, uploaded_at=?7
                     WHERE id=?1",
                    params![id, name, version, manifest_json, wasm_blob, wasm_sha256, now],
                )?;
                (*id, true)
            }
        };

        // 合并后并集配额预检（R8、R9）：从库内现用量出发，对每个包内记录
        // 扣掉被它覆盖的旧值、加上包里的新值——同 key 以包为准只计一次，不是
        // `existing + package` 简单相加。字节口径与 `plugin_data_usage` 一致
        // （`LENGTH(CAST(data AS BLOB))`）。超限整包拒绝，一行不写。
        let mut projected: i64 = tx.query_row(
            "SELECT COALESCE(SUM(LENGTH(CAST(data AS BLOB))),0) FROM plugin_data WHERE plugin_id=?1",
            [plugin_id],
            |r| r.get(0),
        )?;
        for (key, data) in records {
            let existing: i64 = tx
                .query_row(
                    "SELECT LENGTH(CAST(data AS BLOB)) FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
                    params![plugin_id, key],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
            projected = projected - existing + data.len() as i64;
        }
        if projected > PLUGIN_DATA_MAX {
            anyhow::bail!("合并后 plugin_data 总量超过单插件 {} MiB 的配额", PLUGIN_DATA_MAX / 1024 / 1024);
        }

        // 记录合并（包优先 upsert）：包内键写包值，库内其他键不动。
        for (key, data) in records {
            tx.execute(
                "INSERT INTO plugin_data (plugin_id, record_key, data, updated_at) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(plugin_id, record_key) DO UPDATE SET data=?3, updated_at=?4",
                params![plugin_id, key, data, now],
            )?;
        }
        // kv 合并：setting 表按 `plugin.<id>:<key>` 命名空间 upsert（与 `Db::set`
        // 同一冲突语义），前缀在这里补回。
        for (key, value) in kv {
            tx.execute(
                "INSERT INTO setting (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                params![format!("plugin.{plugin_id}:{key}"), value],
            )?;
        }

        let row = tx.query_row(
            &format!("SELECT {} FROM plugin WHERE id=?1", Self::PLUGIN_COLUMNS),
            [id],
            row_to_plugin,
        )?;
        tx.commit()?;
        Ok(DataMergeOutcome { row, replaced, records_merged: records.len(), kv_merged: kv.len() })
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

    /// 一个插件的记录数与总**字节**数(面板空间占用展示用,R11)。
    ///
    /// `LENGTH(CAST(data AS BLOB))` 而不是 `LENGTH(data)`:`length()` 对 TEXT
    /// 返回**字符**数,与 Rust 侧按字节的 `str::len()` 混算会让 CJK 内容少算
    /// 约 3 倍,配额与占用展示一起失真。
    pub fn plugin_data_usage(&self, plugin_id: &str) -> Result<(i64, i64)> {
        Ok(self.conn().query_row(
            "SELECT COUNT(*), COALESCE(SUM(LENGTH(CAST(data AS BLOB))),0) FROM plugin_data WHERE plugin_id=?1",
            params![plugin_id],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
        )?)
    }

    /// 写一行并**在同一条事务里**强制单插件总字节配额。检查与写入分开做时,
    /// 同一插件的两次并发写会各自读到同一份旧用量、各自判定通过,合起来越过
    /// 上限——而配额正是用来限制磁盘占用的。`BEGIN IMMEDIATE` 立刻取写锁,
    /// 两个并发写不会都通过检查。
    ///
    /// 返回 `Ok(None)` 表示超配额且未写入;`Ok(Some(created))` 表示已写入
    /// (`created` 为 true 时是新建而非覆盖)。
    pub fn plugin_data_put_within_quota(
        &self,
        plugin_id: &str,
        key: &str,
        data: &str,
        max_bytes: i64,
    ) -> Result<Option<bool>> {
        let conn = self.conn();
        conn.execute_batch("BEGIN IMMEDIATE")?;
        let attempted = (|| -> Result<Option<bool>> {
            let used: i64 = conn.query_row(
                "SELECT COALESCE(SUM(LENGTH(CAST(data AS BLOB))),0) FROM plugin_data WHERE plugin_id=?1",
                params![plugin_id],
                |r| r.get(0),
            )?;
            let existing: i64 = conn
                .query_row(
                    "SELECT LENGTH(CAST(data AS BLOB)) FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
                    params![plugin_id, key],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0);
            // 覆盖写要先扣掉被替换的旧值,否则反复覆盖同一行会把用量算高。
            if used - existing + data.len() as i64 > max_bytes {
                return Ok(None);
            }
            let existed = existing > 0
                || conn.query_row(
                    "SELECT COUNT(*) FROM plugin_data WHERE plugin_id=?1 AND record_key=?2",
                    params![plugin_id, key],
                    |r| r.get::<_, i64>(0),
                )? > 0;
            conn.execute(
                "INSERT INTO plugin_data (plugin_id, record_key, data, updated_at) VALUES (?1,?2,?3,?4)
                 ON CONFLICT(plugin_id, record_key) DO UPDATE SET data=?3, updated_at=?4",
                params![plugin_id, key, data, Utc::now().timestamp()],
            )?;
            Ok(Some(!existed))
        })();
        match attempted {
            Ok(v) => {
                conn.execute_batch("COMMIT")?;
                Ok(v)
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
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

/// 写一行新插件并把整行读回来。列清单只有这一份：`create_plugin`（测试直接
/// 建行）与 `install_plugin_package`（上传路径）都走它，加列时不会只改一处。
/// 初值固定为「已停用、无错误」——与首次上传的语义一致（KTD10）。
fn insert_plugin_row(
    conn: &rusqlite::Connection,
    plugin_id: &str,
    name: &str,
    version: &str,
    manifest_json: &str,
    wasm_blob: &[u8],
    wasm_sha256: &str,
) -> rusqlite::Result<PluginRow> {
    conn.execute(
        "INSERT INTO plugin (plugin_id, name, version, manifest_json, wasm_blob,
                             wasm_sha256, enabled, status, uploaded_at)
         VALUES (?1,?2,?3,?4,?5,?6,0,'disabled',?7)",
        params![plugin_id, name, version, manifest_json, wasm_blob, wasm_sha256, Utc::now().timestamp()],
    )?;
    conn.query_row(
        &format!("SELECT {} FROM plugin WHERE id=?1", Db::PLUGIN_COLUMNS),
        [conn.last_insert_rowid()],
        row_to_plugin,
    )
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
    use super::{KV_VALUE_MAX, RECORD_MAX};
    use crate::db::db;
    use crate::db::Db;

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

    /// 上传路径的同 plugin_id 升级：版本更高才就地替换——行 id、kv 与
    /// plugin_data 全留，只换包的内容与版本，并拨回停用；版本没提高则拒且不落库。
    #[test]
    fn a_higher_version_replaces_in_place_and_keeps_the_data() {
        let db = db();
        let (first, replaced) =
            db.install_plugin_package("mailer", "Mailer", "1.0.0", "{\"a\":1}", b"m1", "sha1").unwrap();
        assert!(!replaced, "首次安装不是替换");
        assert_eq!((first.id, first.status.as_str()), (1, "disabled"));

        // 插件自己的数据与 kv：替换必须原样留着（这是升级不丢数据的那条保证）。
        db.plugin_data_put("mailer", "node:1", "42").unwrap();
        db.set("plugin.mailer:bot_token", "secret").unwrap();
        db.set_plugin_enabled(first.id, true).unwrap();

        let (second, replaced) =
            db.install_plugin_package("mailer", "Mailer", "1.1.0", "{\"a\":2}", b"m2", "sha2").unwrap();
        assert!(replaced, "版本更高就是替换");
        assert_eq!(second.id, first.id, "替换不换行 id");
        assert_eq!(
            (second.version.as_str(), second.manifest_json.as_str(), second.wasm_sha256.as_str()),
            ("1.1.0", "{\"a\":2}", "sha2")
        );
        assert_eq!(second.wasm_blob, b"m2", "模块字节换成新的");
        assert!(!second.enabled && second.status == "disabled", "替换后回到停用，等操作员重新启用");
        assert_eq!(db.plugin_data_get("mailer", "node:1").unwrap().as_deref(), Some("42"), "插件数据留着");
        assert_eq!(db.get("plugin.mailer:bot_token").as_deref(), Some("secret"), "kv 留着");

        for stale in ["1.1.0", "1.0.0", "0.9"] {
            let refused = db
                .install_plugin_package("mailer", "Mailer", stale, "{}", b"m3", "sha3")
                .expect_err("版本没提高不该替换");
            assert!(refused.to_string().contains("只有版本更高"), "{refused}");
        }
        let kept = db.get_plugin(first.id).unwrap().unwrap();
        assert_eq!(
            (kept.version.as_str(), kept.wasm_sha256.as_str()),
            ("1.1.0", "sha2"),
            "被拒的包一行都不写"
        );
        assert_eq!(db.list_plugins().unwrap().len(), 1, "替换不新增行");

        // 另一个 plugin_id 仍是新增。
        let (other, replaced) =
            db.install_plugin_package("webhook", "Webhook", "0.1", "{}", b"m4", "sha4").unwrap();
        assert!(!replaced);
        assert_ne!(other.id, first.id);
        assert_eq!(db.list_plugins().unwrap().len(), 2);
    }

    /// 导出快照单事务读出插件行、记录与 kv 三份；导入合并把它们按包优先
    /// 语义写回，往返后库内可见状态与导出时一致（U1 的 Db 层半边）。
    #[test]
    fn export_reads_a_snapshot_and_import_merges_it_back() {
        let db = db();
        let (src, _) =
            db.install_plugin_package("fin", "Finance", "1.2", "{\"a\":1}", b"m1", "sha1").unwrap();
        db.plugin_data_put("fin", "node:1", "d1").unwrap();
        db.plugin_data_put("fin", "node:2", "d2").unwrap();
        db.set("plugin.fin:bot_token", "secret").unwrap();
        db.set_plugin_enabled(src.id, true).unwrap();

        let export = db.export_plugin(src.id).unwrap().expect("装过的插件导得出");
        assert_eq!(export.plugin.version, "1.2");
        assert_eq!(export.records, vec![("node:1".into(), "d1".into()), ("node:2".into(), "d2".into())]);
        assert_eq!(export.kv, vec![("bot_token".into(), "secret".into())], "kv 前缀已剥");
        assert!(db.export_plugin(9999).unwrap().is_none(), "不存在的 id 返回 None");

        // 新装：另一个空库风格的 plugin_id，插件行 + kv + 记录全落库、停用。
        let out = db
            .install_plugin_package_with_data(
                "fresh",
                "Fresh",
                "1.0",
                "{}",
                b"m2",
                "sha2",
                &[("k".into(), "v".into())],
                &[("token".into(), "t".into())],
            )
            .unwrap();
        assert!(!out.replaced && !out.row.enabled && out.row.status == "disabled");
        assert_eq!((out.records_merged, out.kv_merged), (1, 1));
        assert_eq!(db.plugin_data_get("fresh", "k").unwrap().as_deref(), Some("v"));
        assert_eq!(db.get("plugin.fresh:token").as_deref(), Some("t"));
    }

    /// 同版本覆盖：包内键恢复包值、包外键保留、库内无删除；重复导入幂等。
    #[test]
    fn same_version_import_merges_package_over_existing_and_is_idempotent() {
        let db = db();
        let (row, _) = db.install_plugin_package("fin", "Finance", "1.2", "{}", b"m", "sha").unwrap();
        db.plugin_data_put("fin", "d1", "old").unwrap();
        db.plugin_data_put("fin", "d2", "keep").unwrap();
        db.set("plugin.fin:token", "old-token").unwrap();
        db.set_plugin_enabled(row.id, true).unwrap();

        // 同版本，携带 d1 的新值 + 新增 d3；d2 与包无关。
        let import = |db: &Db| {
            db.install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.2",
                "{}",
                b"m2",
                "sha2",
                &[("d1".into(), "new".into()), ("d3".into(), "add".into())],
                &[("token".into(), "new-token".into())],
            )
            .unwrap()
        };
        let out = import(&db);
        assert!(out.replaced, "同版本是覆盖");
        assert!(!out.row.enabled && out.row.status == "disabled", "覆盖后回停用待启用");
        assert_eq!(out.row.id, row.id, "覆盖不换行 id");
        assert_eq!(db.plugin_data_get("fin", "d1").unwrap().as_deref(), Some("new"), "包内键恢复包值");
        assert_eq!(db.plugin_data_get("fin", "d2").unwrap().as_deref(), Some("keep"), "包外键保留");
        assert_eq!(db.plugin_data_get("fin", "d3").unwrap().as_deref(), Some("add"), "包内新键新增");
        assert_eq!(db.get("plugin.fin:token").as_deref(), Some("new-token"), "kv 恢复包值");

        // 幂等：同包再导一次，库内值集完全一致，行不新增。
        import(&db);
        assert_eq!(db.plugin_data_list("fin", "").unwrap().len(), 3);
        assert_eq!(db.plugin_data_get("fin", "d1").unwrap().as_deref(), Some("new"));
        assert_eq!(db.list_plugins().unwrap().len(), 1);
    }

    /// 更高版本导入是升级 + 合并；库内更高则整包拒绝、一行不写。
    #[test]
    fn higher_version_upgrades_but_a_lower_version_import_is_refused() {
        let db = db();
        db.install_plugin_package("fin", "Finance", "1.2", "{}", b"m", "sha").unwrap();
        db.plugin_data_put("fin", "d1", "orig").unwrap();

        let up = db
            .install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.3",
                "{}",
                b"m2",
                "sha2",
                &[("d1".into(), "v13".into())],
                &[],
            )
            .unwrap();
        assert!(up.replaced && up.row.version == "1.3");
        assert_eq!(db.plugin_data_get("fin", "d1").unwrap().as_deref(), Some("v13"));

        let refused = db
            .install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.2",
                "{}",
                b"m3",
                "sha3",
                &[("d1".into(), "downgrade".into())],
                &[],
            )
            .expect_err("库内版本更高，含数据包按 R7 拒绝");
        assert!(refused.to_string().contains("更低会拒绝"), "{refused}");
        let kept = db.get_plugin(up.row.id).unwrap().unwrap();
        assert_eq!(kept.version, "1.3", "被拒的包一行不写");
        assert_eq!(db.plugin_data_get("fin", "d1").unwrap().as_deref(), Some("v13"), "记录也不动");
    }

    /// 校验与配额：合并后总量超配额、单条超限、key 为空都整包拒绝，pre/post
    /// 行数一致（R9 的一行不写）；记录 key 含 `:` 是合法形状，不拒。
    #[test]
    fn import_rejects_oversized_or_malformed_data_without_writing_a_row() {
        let db = db();
        db.install_plugin_package("fin", "Finance", "1.0", "{}", b"m", "sha").unwrap();
        db.plugin_data_put("fin", "existing", "x").unwrap();
        let before = db.plugin_data_list("fin", "").unwrap();

        // 单条超 256 KiB。
        let big = "x".repeat(RECORD_MAX + 1);
        let e = db
            .install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.0",
                "{}",
                b"m",
                "sha",
                &[("d".into(), big)],
                &[],
            )
            .expect_err("单条超限");
        assert!(e.to_string().contains("单条"), "{e}");

        // kv value 超 8 KiB。
        let big_kv = "y".repeat(KV_VALUE_MAX + 1);
        assert!(db
            .install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.0",
                "{}",
                b"m",
                "sha",
                &[],
                &[("token".into(), big_kv)],
            )
            .is_err());

        // 空 key。
        assert!(db
            .install_plugin_package_with_data(
                "fin",
                "Finance",
                "1.0",
                "{}",
                b"m",
                "sha",
                &[("".into(), "v".into())],
                &[],
            )
            .is_err());

        // 合并后总量超配额：多条各 200 KiB 累加越过 16 MiB。
        let chunk = "z".repeat(200 * 1024);
        let many: Vec<(String, String)> = (0..90).map(|i| (format!("rec{i}"), chunk.clone())).collect();
        let e = db
            .install_plugin_package_with_data("fin", "Finance", "1.0", "{}", b"m", "sha", &many, &[])
            .expect_err("合并后超配额");
        assert!(e.to_string().contains("配额"), "{e}");

        // 记录 key 含 `:` 合法，放行。
        db.install_plugin_package_with_data(
            "fin",
            "Finance",
            "1.0",
            "{}",
            b"m",
            "sha",
            &[("node:1".into(), "v".into())],
            &[],
        )
        .expect("含 `:` 的记录 key 是合法形状");
        assert_eq!(db.plugin_data_get("fin", "node:1").unwrap().as_deref(), Some("v"));

        // 每一次拒绝都没动 existing 那行。
        assert_eq!(db.plugin_data_get("fin", "existing").unwrap().as_deref(), Some("x"));
        assert!(before.iter().all(|(k, _)| db.plugin_data_get("fin", k).unwrap().is_some()));
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

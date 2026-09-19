//! 内存版 kv / plugin_data / 节点表:契约替身([`crate::contract_state::ContractState`])
//! 的存储层。语义对齐 monitor 的 `db`(`setting` 表的 kv、`plugin_data` 表的
//! CRUD 与配额),但没有任何持久化。

use std::collections::HashMap;
use std::sync::Mutex;

use crate::host::NodeInfo;

/// kv 的命名空间拼接。**唯一一处**定义 `plugin.<plugin_id>:<key>` 的形状:
/// 宿主函数层用它,两个 `Host` 实现因此都只见到拼好的 key。
pub fn setting_key(plugin_id: &str, key: &str) -> String {
    format!("plugin.{plugin_id}:{key}")
}

/// 内存 kv(setting 表的替身)。
#[derive(Default)]
pub struct ContractKv {
    settings: Mutex<HashMap<String, String>>,
    plugin_data: Mutex<HashMap<(String, String), String>>,
}

impl ContractKv {
    pub fn new() -> Self {
        Self::default()
    }

    // ---- kv(setting 表) ----

    /// 读一个值,不存在返回 `None`。
    pub fn try_get(&self, key: &str) -> Option<String> {
        self.settings.lock().unwrap_or_else(|e| e.into_inner()).get(key).cloned()
    }

    /// 写一个值(upsert)。
    pub fn set(&self, key: &str, value: &str) {
        self.settings.lock().unwrap_or_else(|e| e.into_inner()).insert(key.to_owned(), value.to_owned());
    }

    // ---- plugin_data ----

    /// 插入或覆盖一行,并强制单插件总字节配额。返回 `false` 表示超配额、未写入。
    ///
    /// 覆盖写先扣掉被替换的旧值,与 monitor `db::plugin_data_put_within_quota`
    /// 同一套算法(否则反复覆盖同一行会把用量算高)。
    pub fn plugin_data_put_within_quota(
        &self,
        plugin_id: &str,
        key: &str,
        data: &str,
        max_bytes: i64,
    ) -> bool {
        let mut rows = self.plugin_data.lock().unwrap_or_else(|e| e.into_inner());
        let used: i64 =
            rows.iter().filter(|((pid, _), _)| pid == plugin_id).map(|(_, v)| v.len() as i64).sum();
        let existing = rows.get(&(plugin_id.to_owned(), key.to_owned())).map(|v| v.len() as i64).unwrap_or(0);
        if used - existing + data.len() as i64 > max_bytes {
            return false;
        }
        rows.insert((plugin_id.to_owned(), key.to_owned()), data.to_owned());
        true
    }

    /// 读一行。
    pub fn plugin_data_get(&self, plugin_id: &str, key: &str) -> Option<String> {
        self.plugin_data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(plugin_id.to_owned(), key.to_owned()))
            .cloned()
    }

    /// 删一行(本就无此记录也算成功)。
    pub fn plugin_data_delete(&self, plugin_id: &str, key: &str) {
        self.plugin_data
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&(plugin_id.to_owned(), key.to_owned()));
    }

    /// 按前缀列出,按 key 排序(与 monitor 的 `ORDER BY record_key` 一致)。
    pub fn plugin_data_list(&self, plugin_id: &str, prefix: &str) -> Vec<(String, String)> {
        let rows = self.plugin_data.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<(String, String)> = rows
            .iter()
            .filter(|((pid, key), _)| pid == plugin_id && key.starts_with(prefix))
            .map(|((_, key), data)| (key.clone(), data.clone()))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    /// 一个插件的 `(记录数, 总字节数)`。
    pub fn plugin_data_usage(&self, plugin_id: &str) -> (i64, i64) {
        let rows = self.plugin_data.lock().unwrap_or_else(|e| e.into_inner());
        rows.iter()
            .filter(|((pid, _), _)| pid == plugin_id)
            .fold((0i64, 0i64), |(n, bytes), (_, v)| (n + 1, bytes + v.len() as i64))
    }
}

/// 内存节点表 + 在线集合。`online` 由在线集合推出,与真宿主
/// (`app.agents` 的会话 keys)同一套语义。
#[derive(Default)]
pub struct ContractNodes {
    nodes: Mutex<HashMap<i64, String>>,
    online: Mutex<Vec<i64>>,
}

impl ContractNodes {
    pub fn new() -> Self {
        Self::default()
    }

    /// 播一台节点(默认离线)。
    pub fn add_node(&self, id: i64, name: &str) {
        self.nodes.lock().unwrap_or_else(|e| e.into_inner()).insert(id, name.to_owned());
    }

    /// 置一台节点的在线状态。
    pub fn set_online(&self, id: i64, online: bool) {
        let mut set = self.online.lock().unwrap_or_else(|e| e.into_inner());
        set.retain(|&x| x != id);
        if online {
            set.push(id);
        }
    }

    /// 全部节点,按 id 排序。
    pub fn list(&self) -> Vec<NodeInfo> {
        let nodes = self.nodes.lock().unwrap_or_else(|e| e.into_inner());
        let online = self.online.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<NodeInfo> = nodes
            .iter()
            .map(|(&id, name)| NodeInfo { id, name: name.clone(), online: online.contains(&id) })
            .collect();
        out.sort_by_key(|n| n.id);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 命名空间拼接只有这一处,两个实现共用同一串。key 里含 `:` 的旧写法不再
    /// 需要(前缀是 plugin_id 而不是 key)。
    #[test]
    fn the_setting_key_carries_the_plugin_namespace() {
        assert_eq!(setting_key("com.example.test", "mykey"), "plugin.com.example.test:mykey");
    }

    /// plugin_data 按插件隔离、按前缀过滤、按 key 排序;覆盖写不吃配额里的旧值。
    #[test]
    fn plugin_data_isolates_filters_sorts_and_counts_usage() {
        let kv = ContractKv::new();
        assert!(kv.plugin_data_put_within_quota("a", "node:2", "bb", 100));
        assert!(kv.plugin_data_put_within_quota("a", "node:1", "c", 100));
        assert!(kv.plugin_data_put_within_quota("a", "fx", "d", 100));
        assert!(kv.plugin_data_put_within_quota("b", "node:1", "keep", 100));
        assert_eq!(
            kv.plugin_data_list("a", "node:"),
            vec![("node:1".to_owned(), "c".to_owned()), ("node:2".to_owned(), "bb".to_owned())]
        );
        assert_eq!(kv.plugin_data_list("a", "").len(), 3);
        assert_eq!(kv.plugin_data_get("b", "node:1").as_deref(), Some("keep"), "插件之间互不可见");
        assert_eq!(kv.plugin_data_usage("a"), (3, 4));
        // 覆盖写:用量仍是 4 字节而不是累加。
        assert!(kv.plugin_data_put_within_quota("a", "node:2", "z", 100));
        assert_eq!(kv.plugin_data_usage("a"), (3, 3));
        kv.plugin_data_delete("a", "node:2");
        assert_eq!(kv.plugin_data_usage("a").0, 2);
    }

    /// 总配额:超一点都拒,且不写入。
    #[test]
    fn plugin_data_quota_refuses_without_writing() {
        let kv = ContractKv::new();
        assert!(kv.plugin_data_put_within_quota("a", "k", "0123456789", 10));
        assert!(!kv.plugin_data_put_within_quota("a", "k2", "x", 10), "已满 10 字节,再加 1 字节超限");
        assert_eq!(kv.plugin_data_get("a", "k2"), None, "被拒的写不留痕迹");
        // 覆盖写替换的是自己的旧值,不撞配额。
        assert!(kv.plugin_data_put_within_quota("a", "k", "0123456789", 10));
    }

    /// 节点的在线状态由在线集合推出,与节点本身是否播过无关。
    #[test]
    fn nodes_report_their_online_state() {
        let nodes = ContractNodes::new();
        nodes.add_node(2, "edge-down");
        nodes.add_node(1, "edge-up");
        nodes.set_online(1, true);
        let list = nodes.list();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], NodeInfo { id: 1, name: "edge-up".into(), online: true });
        assert_eq!(list[1], NodeInfo { id: 2, name: "edge-down".into(), online: false });
    }
}

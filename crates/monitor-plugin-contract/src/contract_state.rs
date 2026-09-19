//! 契约测试用的宿主替身:内存 kv / plugin_data / 节点表 + 可换 http 后端 +
//! 嵌入的预算与 scratch。
//!
//! 它不是"`App` 的子集",而是"契约测试需要的最小状态集"——`App` 携 db
//! pool/http client/config/registry,契约 crate 一条都不该有(见 crate 文档)。
//! plugin 仓 release 时用它 `Linker::instantiate` 真插件 wasm,等于用真宿主的
//! 13 个函数跑一遍。

use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::constants::PLUGIN_DATA_MAX;
use crate::host::{HasScratch, Host, HttpMethod, NodeInfo};
use crate::http::{ContractHttp, NoopHttp};
use crate::kv::{ContractKv, ContractNodes};

/// 契约测试的宿主状态。
pub struct ContractState {
    plugin_id: String,
    kv: ContractKv,
    nodes: ContractNodes,
    http: Box<dyn ContractHttp>,
    deadline: Instant,
    resp_ptr: i32,
    resp_cap: i32,
    logs: Mutex<Vec<String>>,
    events: Mutex<Vec<(String, Value)>>,
}

impl ContractState {
    /// 一个插件身份的替身,默认:空存储、无节点、http 全拒([`NoopHttp`])、
    /// 60 秒预算、无日志。
    ///
    /// 60 秒是"给测试足够跑完"的量:真宿主的 5 秒派发预算对 `instantiate` 一节
    /// 的驱动不适用,而预算检查本身的测试用 [`ContractState::with_deadline`]
    /// 显式把截止点放到过去。
    pub fn for_test(plugin_id: &str) -> Self {
        Self {
            plugin_id: plugin_id.to_owned(),
            kv: ContractKv::new(),
            nodes: ContractNodes::new(),
            http: Box::new(NoopHttp),
            deadline: Instant::now() + Duration::from_secs(60),
            resp_ptr: 0,
            resp_cap: 0,
            logs: Mutex::new(Vec::new()),
            events: Mutex::new(Vec::new()),
        }
    }

    /// 覆写墙钟截止点(测预算耗尽用)。
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = deadline;
        self
    }

    /// 装上 http 后端(默认是 `NoopHttp`)。
    pub fn with_http(mut self, http: Box<dyn ContractHttp>) -> Self {
        self.http = http;
        self
    }

    /// 就地换 http 后端。
    pub fn set_http(&mut self, http: Box<dyn ContractHttp>) {
        self.http = http;
    }

    pub fn kv(&self) -> &ContractKv {
        &self.kv
    }

    pub fn nodes(&self) -> &ContractNodes {
        &self.nodes
    }

    /// 插件经 `host.log` 打过的日志,按发生顺序。
    pub fn logs(&self) -> Vec<String> {
        self.logs.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// 插件经 `host.emit_event` 发过的事件,按发生顺序。
    pub fn events(&self) -> Vec<(String, Value)> {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl Host for ContractState {
    fn kv_get(&self, key: &str) -> Result<Option<String>, ()> {
        Ok(self.kv.try_get(key))
    }

    fn kv_set(&self, key: &str, value: &str) -> Result<(), ()> {
        self.kv.set(key, value);
        Ok(())
    }

    fn data_put(&self, plugin_id: &str, key: &str, value: &str) -> Result<bool, ()> {
        Ok(self.kv.plugin_data_put_within_quota(plugin_id, key, value, PLUGIN_DATA_MAX))
    }

    fn data_get(&self, plugin_id: &str, key: &str) -> Result<Option<String>, ()> {
        Ok(self.kv.plugin_data_get(plugin_id, key))
    }

    fn data_delete(&self, plugin_id: &str, key: &str) -> Result<(), ()> {
        self.kv.plugin_data_delete(plugin_id, key);
        Ok(())
    }

    fn data_list(&self, plugin_id: &str, prefix: &str) -> Result<Vec<(String, String)>, ()> {
        Ok(self.kv.plugin_data_list(plugin_id, prefix))
    }

    fn nodes_query(&self) -> Result<Vec<NodeInfo>, ()> {
        Ok(self.nodes.list())
    }

    /// 记下来、返回成功:替身没有总线可发,但契约测试要能断言"插件确实发了
    /// 这条事件"。真宿主在这一步走 `notification_bus::emit`。
    fn emit_event(&self, name: &str, payload: Value) -> Result<(), ()> {
        self.events.lock().unwrap_or_else(|e| e.into_inner()).push((name.to_owned(), payload));
        Ok(())
    }

    fn http_request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        download_cap: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, i32> {
        // 有界下载也照做:替身不该让插件拿到超过 download_cap 的字节,否则契约
        // 测试会放过一个在真宿主上必然被截断的插件。
        let mut body = self.http.request(method, url, body, deadline)?;
        body.truncate(download_cap);
        Ok(body)
    }
}

impl HasScratch for ContractState {
    fn plugin_id(&self) -> &str {
        &self.plugin_id
    }

    fn deadline(&self) -> Instant {
        self.deadline
    }

    fn resp(&self) -> (i32, i32) {
        (self.resp_ptr, self.resp_cap)
    }

    fn set_resp(&mut self, ptr: i32, cap: i32) {
        self.resp_ptr = ptr;
        self.resp_cap = cap;
    }

    fn push_log(&self, text: &str) {
        self.logs.lock().unwrap_or_else(|e| e.into_inner()).push(text.to_owned());
    }
}

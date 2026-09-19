//! 宿主函数看得见的宿主侧抽象。两个 trait 把 13 个宿主函数与它们的宿主状态
//! 解耦:monitor 主 crate 用 `PluginState`(委托 `Arc<App>`)实现,契约测试用
//! [`crate::contract_state::ContractState`](内存后端)实现。同一份闭包实现
//! (见 [`crate::host_linker::linker`])编译进两边——签名、返回码、限额改动
//! 一边不同两边都编译不过。

use std::time::Instant;

/// 插件 http 请求的方法。
///
/// 用枚举而不是 `reqwest::Method`:契约 crate 不依赖 reqwest(它的依赖只有
/// wasmtime/serde/serde_json/chrono),真宿主在自己的实现里把它映射回
/// `reqwest::Method`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpMethod {
    Get,
    Post,
}

/// `nodes_query` 回给插件的一行:只有 id/name/online/created_at 四个字段。
///
/// 财务字段(price/currency/...)自 v2 起归财务插件的 plugin_data,宿主不认
/// 它们,也不该认。
///
/// `created_at` 是节点的身份:SQLite 会把已删节点的 id 交给下一个新建的节点,
/// 订阅者靠这一对 (id, created_at) 把「同一台机器」与「同一个 id」分开。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub id: i64,
    pub name: String,
    pub online: bool,
    pub created_at: i64,
}

/// 10 个碰宿主数据面的操作(13 个宿主函数里除 `log`/`now`/`resp_alloc` 外
/// 的全部)。每个方法对应原闭包体里对 `app` 的一段访问。
///
/// `Err(())` 一律是"宿主侧存储/网络失败"(由实现自己把它折成返回值对应的
/// 错误码并记日志),闭包层不再区分原因——具体的错误文本由实现在自己那一侧
/// 打点,那里才拿得到错误链。
///
/// `()` 是**有意的**:这些方法的失败只有一个去向——闭包把它折成一个固定的数字
/// 错误码回到 wasm;取消原因在实现那一侧已经落进日志(错误链到不了闭包)。
/// 换成自定义错误类型只会在闭包里多一个没人读的字段。
#[allow(clippy::result_unit_err)]
pub trait Host {
    /// kv 读。key 是**已拼好命名空间**的(`plugin.<plugin_id>:<key>`),拼接在
    /// 宿主函数层单点完成(见 [`crate::kv::setting_key`])。
    fn kv_get(&self, key: &str) -> Result<Option<String>, ()>;

    /// kv 写。key 同 [`Host::kv_get`]。
    fn kv_set(&self, key: &str, value: &str) -> Result<(), ()>;

    /// plugin_data 写(带单插件总配额)。`Ok(false)` 表示超配额、未写入。
    fn data_put(&self, plugin_id: &str, key: &str, value: &str) -> Result<bool, ()>;

    /// plugin_data 读。
    fn data_get(&self, plugin_id: &str, key: &str) -> Result<Option<String>, ()>;

    /// plugin_data 删(本就无此记录也算成功)。
    fn data_delete(&self, plugin_id: &str, key: &str) -> Result<(), ()>;

    /// plugin_data 按前缀列出,按 key 排序。
    fn data_list(&self, plugin_id: &str, prefix: &str) -> Result<Vec<(String, String)>, ()>;

    /// 节点基础信息(只读)。
    fn nodes_query(&self) -> Result<Vec<NodeInfo>, ()>;

    /// 发一条 `plugin_` 事件到总线。事件名合法性(前缀、非空)由宿主函数层
    /// 先判,这里只负责投递。
    fn emit_event(&self, name: &str, payload: serde_json::Value) -> Result<(), ()>;

    /// 发一次插件 http 请求(含 SSRF 预检与有界下载),返回响应体。
    /// `Err(码)` 是直接回给插件的错误码(-2/-4/-5/-9)。
    ///
    /// 真宿主把整段 `plugin_http_fetch`(预检 + 预算收缩 + 有界读取)委托在
    /// 这里;契约替身把请求交给可插拔的 [`crate::http::ContractHttp`]。
    fn http_request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        download_cap: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, i32>;
}

/// 每次调用的"草稿纸":插件身份、墙钟预算、http 响应缓冲与日志汇集点。
/// 3 个不碰宿主数据的宿主函数(`log`/`now`/`resp_alloc` 里的前两个半)只用
/// 这里的东西。
///
/// `log` 只经 [`HasScratch::push_log`] 落到调用方持有的汇集点上,清洗与截断
/// 留在那一侧(见 monitor 的 `plugin::log`)——契约 crate 不认识面板的
/// detail 格式。
pub trait HasScratch {
    /// 本次调用的插件身份(kv 命名空间、日志前缀都用它)。
    fn plugin_id(&self) -> &str;
    /// 本次调用的墙钟预算截止点。
    fn deadline(&self) -> Instant;
    /// 最近一次 `host_resp_alloc` 记下的缓冲 `(ptr, cap)`。
    fn resp(&self) -> (i32, i32);
    /// 记下 `host_resp_alloc` 拿到的缓冲。
    fn set_resp(&mut self, ptr: i32, cap: i32);
    /// 插件经 `host.log` 打的一句话。越界/非法 UTF-8 的处置由调用方在调用前
    /// 完成(那种情况下宿主函数不会调到这里)。
    fn push_log(&self, text: &str);
}

//! 引擎与加载(R6)、调用骨架与事件派发入口。宿主函数面的**实现**在
//! `monitor-plugin-contract` crate(与契约测试同一份闭包),本模块的
//! `PluginState` 实现它的 `Host`/`HasScratch`,把每个宿主操作委托回 `Arc<App>`;
//! 插件日志汇集见 [`super::log`]。

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tracing::warn;
use wasmtime::{Memory, Store};

use monitor_plugin_contract::{HasScratch, Host, HttpMethod, NodeInfo};

use crate::notification_bus::Event;
use crate::{db::PluginRow, App};

use super::host_funcs::{host_linker, plugin_http_fetch};
use super::log::{new_log_sink, LogSink};
use super::manifest::Manifest;

// ---------------------------------------------------------------------------
// 引擎与加载(R6)
// ---------------------------------------------------------------------------

/// 进程唯一的 wasmtime 引擎。fuel 计量必须在 Config 上显式开启,否则
/// `Store::set_fuel` 静默无效,死循环插件不会被中断。
pub fn new_engine() -> wasmtime::Engine {
    let mut config = wasmtime::Config::new();
    config.consume_fuel(true);
    wasmtime::Engine::new(&config).expect("构建 wasmtime Engine 不应失败")
}

/// 一个加载完毕、可以反复调用的插件:manifest 与编译产物。
///
/// 只缓存 `Module`(Send+Sync,编译结果在 Engine 里共享,clone 是 Arc 语义);
/// 不持有 instance/store,原因见模块文档的"资源模型"。派发把整个
/// `LoadedPlugin` clone 进任务:manifest 与 Module 都是 Arc 克隆,廉价。
#[derive(Clone)]
pub struct LoadedPlugin {
    pub manifest: Manifest,
    module: wasmtime::Module,
}

impl std::fmt::Debug for LoadedPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoadedPlugin").field("manifest", &self.manifest).finish_non_exhaustive()
    }
}

/// 把 db 行变成可调用的插件:解析 manifest、编译 wasm、检查导出契约。
/// manifest 与模块二进制各自独立报错——上传 API 需要区分"manifest 写错了"和
/// "模块编译不过"。
pub fn load(engine: &wasmtime::Engine, row: &PluginRow) -> Result<LoadedPlugin> {
    let manifest = Manifest::parse(&row.manifest_json)
        .with_context(|| format!("插件 {}({}) 的 manifest 无效", row.id, row.plugin_id))?;
    let module = wasmtime::Module::new(engine, &row.wasm_blob[..])
        .map_err(|e| anyhow::anyhow!("插件 {}({}) 的 wasm 模块编译失败: {e}", row.id, row.plugin_id))?;
    // 导出契约在加载时检查而不是调用时:一次上传、尽早暴露,调用路径上不再有
    // "模块长得不对"这种配置型错误。v2 在 v1 的三项之外,按 manifest 声明
    // 检查 tick/page/cleanup 对应的导出(KTD12)——声明了能力却缺导出是配置
    // 型错误,与缺 on_event 同等对待。
    use wasmtime::ExternType;
    if !matches!(module.get_export("memory"), Some(ExternType::Memory(_))) {
        bail!("插件 {} 的模块缺少导出 `memory`", manifest.plugin_id);
    }
    let mut required: Vec<&str> = vec!["on_event", "__alloc"];
    if manifest.tick {
        required.push("on_tick");
    }
    if manifest.page.is_some() {
        required.extend(["render_page", "on_action"]);
    }
    if manifest.cleanup {
        required.push("on_cleanup");
    }
    for name in required {
        if !matches!(module.get_export(name), Some(ExternType::Func(_))) {
            bail!("插件 {} 的模块缺少导出 `{name}`(manifest 声明了该能力)", manifest.plugin_id);
        }
    }
    Ok(LoadedPlugin { manifest, module })
}

// ---------------------------------------------------------------------------
// 上限、每次调用的状态与实例化
// ---------------------------------------------------------------------------

// 限额与错误码的**唯一定义**在 `monitor-plugin-contract`:那里是 13 个宿主函数的
// 实现,这里是它们的调用方与 `Host` 实现。两处各抄一份迟早会漂,所以这里只
// re-export,不重新定义(契约 crate 的 `constants` / `error_codes`)。
pub(super) use monitor_plugin_contract::constants::HTTP_TIMEOUT;
use monitor_plugin_contract::constants::PLUGIN_DATA_MAX;
pub use monitor_plugin_contract::constants::{
    DEFAULT_FUEL_LIMIT, DEFAULT_HOOK_FUEL_LIMIT, KV_KEY_MAX, KV_VALUE_MAX,
};

/// 一个 kv key 的形状问题。key 的语法只有一套,[`kv_key_problem`] 是它唯一的
/// 判定点;两个调用方(manifest 的 `[[kv]]` 校验、面板的 kv 编辑器)只是把结果
/// 折成自己的文案,不再各写一遍规则。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvKeyProblem {
    /// 空,或只有空白。
    Empty,
    /// 首尾带空白。面板/接口写入的是**原样** key,带空白的那一行与 manifest
    /// 声明永远对不上——与其让「声明了却配不上」变成一个谜,不如在两侧都拒掉。
    Padded,
    /// 含 `':'`:它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符。
    Colon,
    /// 超过 [`KV_KEY_MAX`] 字节。
    TooLong,
}

/// 判一个 kv key 的形状,`None` 表示合法。规则与 kv 命名空间、manifest 声明、
/// 面板写入共用同一份。
pub fn kv_key_problem(key: &str) -> Option<KvKeyProblem> {
    if key.trim().is_empty() {
        return Some(KvKeyProblem::Empty);
    }
    if key != key.trim() {
        return Some(KvKeyProblem::Padded);
    }
    if key.contains(':') {
        return Some(KvKeyProblem::Colon);
    }
    if key.len() > KV_KEY_MAX {
        return Some(KvKeyProblem::TooLong);
    }
    None
}

/// Store 的 user data:宿主函数看得见的全部宿主侧状态。
///
/// 它自己不含宿主函数实现——那 13 个闭包在 `monitor-plugin-contract` 里,泛型在
/// `Host`/`HasScratch` 上;这里实现这两个 trait,每个方法委托回 [`App`](crate::App),
/// 所以运行时行为与闭包直接摸 `app` 时逐字一致。
pub(crate) struct PluginState {
    /// kv 命名空间隔离:`plugin.<plugin_id>:<key>`。
    pub(super) plugin_id: String,
    /// 宿主回程:kv 与 http 都要它。App 本就活在 Arc 里,克隆只是引用计数。
    pub(super) app: Arc<App>,
    /// 本次 `on_event` 的墙钟预算截止点(由 `instantiate` 从 timeout_ms 折算)。
    /// 外层 tokio 超时只能 detach 后台任务,fuel 又只计量 wasm 指令——宿主调用
    /// 循环(每轮 wasm 指令极少、宿主侧执行再久也不耗 fuel)只有这道检查能拦。
    pub(super) deadline: std::time::Instant,
    /// 最近一次 `host_resp_alloc` 拿到的缓冲。`host_http_post` 的 resp_ptr 传 0
    /// 时回落到这里,插件可以少传两个参数。
    pub(super) resp_ptr: i32,
    pub(super) resp_cap: i32,
    /// 本次调用里插件经 `host.log` 打出的日志。见 [`super::log::PluginLog`]。
    pub(super) logs: LogSink,
}

// ---------------------------------------------------------------------------
// 宿主契约的真宿主一侧(KTD1)
// ---------------------------------------------------------------------------
//
// 13 个宿主函数的闭包体在 `monitor-plugin-contract` 里,泛型在这两个 trait 上;
// 这里把每个操作委托回 `Arc<App>`,与闭包当年直接摸 `app` 逐字同义。宿主侧的
// 失败原因(读库失败、emit 失败、http 出错)在这一侧打点——闭包只拿得到
// `Err(())`,错误链到不了那边。

impl Host for PluginState {
    fn kv_get(&self, key: &str) -> Result<Option<String>, ()> {
        self.app.db.try_get(key).map_err(|e| {
            warn!(plugin = %self.plugin_id, "kv_get 读库失败: {e:#}");
        })
    }

    fn kv_set(&self, key: &str, value: &str) -> Result<(), ()> {
        // 写库失败不给 -8 而是 -2(kv 的错误码表见契约 crate 的 error_codes),
        // 这一层只报"失败",折码在闭包那边。
        self.app.db.set(key, value).map_err(|_| ())
    }

    fn data_put(&self, plugin_id: &str, key: &str, value: &str) -> Result<bool, ()> {
        // 配额检查与写入在 db 层的同一事务里做:分成"读用量→判→写"三步时,同一
        // 插件的两次并发写会各自通过检查,合起来越过上限(KTD3)。
        match self.app.db.plugin_data_put_within_quota(plugin_id, key, value, PLUGIN_DATA_MAX) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => {
                warn!(plugin = %plugin_id, "data_put 失败: {e:#}");
                Err(())
            }
        }
    }

    fn data_get(&self, plugin_id: &str, key: &str) -> Result<Option<String>, ()> {
        // 读失败并进「无此记录」:data_get 的错误码表里没有"库坏了"这一档,旧
        // 插件把任何 <=0 都当无值,行为保持不变。
        Ok(self.app.db.plugin_data_get(plugin_id, key).unwrap_or(None))
    }

    fn data_delete(&self, plugin_id: &str, key: &str) -> Result<(), ()> {
        match self.app.db.plugin_data_delete(plugin_id, key) {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!(plugin = %plugin_id, "data_delete 失败: {e:#}");
                Err(())
            }
        }
    }

    fn data_list(&self, plugin_id: &str, prefix: &str) -> Result<Vec<(String, String)>, ()> {
        match self.app.db.plugin_data_list(plugin_id, prefix) {
            Ok(rows) => Ok(rows),
            Err(e) => {
                warn!(plugin = %plugin_id, "data_list 失败: {e:#}");
                Err(())
            }
        }
    }

    fn nodes_query(&self) -> Result<Vec<NodeInfo>, ()> {
        let online: HashSet<i64> =
            self.app.agents.read().unwrap_or_else(|e| e.into_inner()).keys().copied().collect();
        match self.app.db.nodes() {
            Ok(nodes) => Ok(nodes
                .iter()
                .map(|n| NodeInfo { id: n.id, name: n.name.clone(), online: online.contains(&n.id) })
                .collect()),
            Err(e) => {
                warn!(plugin = %self.plugin_id, "host_nodes_query 读节点失败: {e:#}");
                Err(())
            }
        }
    }

    fn emit_event(&self, name: &str, payload: serde_json::Value) -> Result<(), ()> {
        let event = Event::Plugin { name: name.to_owned(), payload };
        if let Err(e) = crate::notification_bus::emit(&self.app, &event) {
            warn!(plugin = %self.plugin_id, "emit_event 失败: {e:#}");
            return Err(());
        }
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
        // 请求的构造、SSRF 预检、预算收缩与有界下载都在 `plugin_http_fetch` 里
        // ——它要 `App::plugin_http`(不跟随重定向的那个 client),所以留在真宿主
        // 这一侧,没有进契约 crate。
        let (label, method) = match method {
            HttpMethod::Get => ("host_http_get", reqwest::Method::GET),
            HttpMethod::Post => ("host_http_post", reqwest::Method::POST),
        };
        let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
            warn!(plugin = %self.plugin_id, "{label} 不在异步运行时上下文中");
            return Err(-4);
        };
        handle.block_on(plugin_http_fetch(
            &self.app,
            &self.plugin_id,
            label,
            method,
            url,
            body,
            download_cap,
            deadline,
        ))
    }
}

impl HasScratch for PluginState {
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
        // 清洗与截断留在汇集点那一侧(见 [`super::log`]):它认识面板的 detail
        // 契约,契约 crate 不认识。
        self.logs.lock().unwrap_or_else(|e| e.into_inner()).push(text);
    }
}

/// 把调用侧的 Store/内存组合暴露给 tests:测试需要直接看内存与 db,而
/// `call_on_event` 只返回 i32。
pub(crate) struct InstanceHandle {
    pub store: Store<PluginState>,
    pub instance: wasmtime::Instance,
}

/// 新建 Store(带 fuel 与墙钟 deadline)、注册宿主函数、实例化。`call_on_event`
/// 的骨架,也是测试直接驱动单个宿主函数的入口。
///
/// `logs` 由调用方建、调用方留一份:Store 随本次调用消失,而插件日志要在那之后
/// 才读得到,超时被放弃的任务更是只剩调用方手里这一个 [`LogSink`]。
pub(crate) fn instantiate(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin_id: &str,
    module: &wasmtime::Module,
    fuel_limit: u64,
    timeout_ms: u64,
    logs: &LogSink,
) -> Result<InstanceHandle> {
    let mut store = Store::new(
        engine,
        PluginState {
            plugin_id: plugin_id.into(),
            app: app.clone(),
            deadline: std::time::Instant::now() + Duration::from_millis(timeout_ms),
            resp_ptr: 0,
            resp_cap: 0,
            logs: Arc::clone(logs),
        },
    );
    // fuel 在任何 wasm 执行(含 start 段)之前就位。
    store.set_fuel(fuel_limit)?;
    let linker = host_linker(engine)?;
    let instance = linker.instantiate(&mut store, module)?;
    Ok(InstanceHandle { store, instance })
}

// ---------------------------------------------------------------------------
// 事件派发入口
// ---------------------------------------------------------------------------

/// 把一个事件交给插件处理,返回 `on_event` 的 i32(0 = 成功,非 0 = 插件自定义
/// 错误码)。trap(fuel 耗尽、越界访问等)返回 `Err`。
///
/// 超时不在这一层强制中断:Registry 的派发循环在外面套 tokio 超时,超时只是
/// 不再等后台任务;fuel 只计量 wasm 指令,拦不住宿主调用循环。所以把
/// `timeout_ms` 折算成 deadline 存进 PluginState,宿主函数(http_post/kv_get)
/// 入口检查——预算耗尽后宿主调用被拒绝,wasm 循环每轮拿到负数返回值,配合
/// fuel 兜底,两种死循环都出得来。http 的单请求超时见 [`HTTP_TIMEOUT`]。
///
/// `logs` 是本次调用的日志汇集点,由调用方持有——返回 i32 带不出任何东西,超时
/// 路径上连返回值都没有。
pub fn call_on_event(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    event: &Event,
    fuel_limit: u64,
    timeout_ms: u64,
    logs: &LogSink,
) -> Result<i32> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms, logs)?;
    let payload = serde_json::to_vec(event)?;
    let alloc = handle
        .instance
        .get_typed_func::<(i32,), i32>(&mut handle.store, "__alloc")
        .map_err(|_| anyhow::anyhow!("模块缺少 __alloc(load 已检查,不应到达这里)"))?;
    let on_event = handle
        .instance
        .get_typed_func::<(i32, i32), i32>(&mut handle.store, "on_event")
        .map_err(|_| anyhow::anyhow!("模块缺少 on_event(load 已检查,不应到达这里)"))?;
    // 载荷缓冲与 host_resp_alloc 走同一个 allocator:宿主从不自己挑地址。
    let ptr = alloc.call(&mut handle.store, (payload.len() as i32,))?;
    if ptr <= 0 {
        bail!("__alloc 返回了非正指针 {ptr}");
    }
    let mem: Memory = handle
        .instance
        .get_memory(&mut handle.store, "memory")
        .context("模块缺少 memory(load 已检查,不应到达这里)")?;
    let start = ptr as usize;
    let end = start.checked_add(payload.len()).context("载荷地址溢出")?;
    let data = mem.data_mut(&mut handle.store);
    if end > data.len() {
        bail!("__alloc 指针 {ptr} 超出线性内存");
    }
    data[start..end].copy_from_slice(&payload);
    Ok(on_event.call(&mut handle.store, (ptr, payload.len() as i32))?)
}

/// 把一段同步的 wasm 执行挪出调度线程,同时保留运行时上下文——宿主函数要
/// `Handle::try_current` 才认得 `app.http`,而 `block_in_place` 正是"离开调度
/// 线程但仍在运行时内"的唯一手段。
///
/// 必须这么做,否则宿主函数里的 `Handle::block_on` 会在已 entered 的上下文里
/// 嵌套 `block_on` 并 panic("Cannot start a runtime from within a runtime")。
/// release 档 `panic = "abort"`,那会直接终止进程。
///
/// 只在多线程运行时上包一层:单线程运行时 `block_in_place` 自身就 panic,而
/// 完全没有运行时(纯同步测试)时也没有嵌套 `block_on` 可言。生产用的是多线程
/// 运行时(main),两条测试/同步路径直接跑。
fn run_blocking<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()) {
        Ok(tokio::runtime::RuntimeFlavor::MultiThread) => tokio::task::block_in_place(f),
        _ => f(),
    }
}

/// 调用一个无参 `() -> i32` 导出(`on_tick`),与 [`call_on_event`] 同一套
/// fuel/deadline 隔离。tick 不带事件载荷:插件在 on_tick 里经宿主函数
/// (nodes_query/data_*/http_get/emit_event)自取所需(U4/KTD4)。
pub fn call_hook(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    hook: &str,
    fuel_limit: u64,
    timeout_ms: u64,
    logs: &LogSink,
) -> Result<i32> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms, logs)?;
    let func = handle
        .instance
        .get_typed_func::<(), i32>(&mut handle.store, hook)
        .map_err(|_| anyhow::anyhow!("模块缺少 {hook}(load 已按 manifest 声明检查,不应到达这里)"))?;
    run_blocking(|| Ok(func.call(&mut handle.store, ())?))
}

/// 调用一个「入参 JSON、返回 JSON」的导出(`render_page`/`on_action`/
/// `on_cleanup`),与 [`call_on_event`] 同一套 fuel/deadline 隔离(U5/U9)。
/// 约定:导出签名 `(ptr: i32, len: i32) -> i32`,返回值是写回 host_resp_alloc
/// 缓冲的响应字节数(0 表示空响应,负数是插件自定义错误码)。宿主用最近一次
/// host_resp_alloc 记下的缓冲读回响应体。
///
/// 日志 sink 在函数体内建、用完即弃:页面/清理的失败已由调用方转成 502 与一条
/// 宿主 warn,detail 没有任何读者,不必让调用方多传一个没人看的参数。
pub fn call_json_hook(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    hook: &str,
    input: &[u8],
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<Vec<u8>> {
    let mut handle = instantiate(
        engine,
        app,
        &plugin.manifest.plugin_id,
        &plugin.module,
        fuel_limit,
        timeout_ms,
        &new_log_sink(),
    )?;
    let alloc = handle
        .instance
        .get_typed_func::<(i32,), i32>(&mut handle.store, "__alloc")
        .map_err(|_| anyhow::anyhow!("模块缺少 __alloc"))?;
    let func = handle
        .instance
        .get_typed_func::<(i32, i32), i32>(&mut handle.store, hook)
        .map_err(|_| anyhow::anyhow!("模块缺少 {hook}(load 已按 manifest 声明检查)"))?;
    let mem: Memory = handle.instance.get_memory(&mut handle.store, "memory").context("模块缺少 memory")?;
    // 入参写进插件内存:与事件载荷同一 allocator 回环。
    let in_ptr = alloc.call(&mut handle.store, (input.len().max(1) as i32,))?;
    if in_ptr <= 0 {
        bail!("__alloc 返回非正指针 {in_ptr}");
    }
    let start = in_ptr as usize;
    let end = start.checked_add(input.len()).context("入参地址溢出")?;
    if end > mem.data(&handle.store).len() {
        bail!("__alloc 指针 {in_ptr} 超出线性内存");
    }
    mem.data_mut(&mut handle.store)[start..end].copy_from_slice(input);
    let n = run_blocking(|| func.call(&mut handle.store, (in_ptr, input.len() as i32)))?;
    if n < 0 {
        bail!("{hook} 返回错误码 {n}");
    }
    // 响应体在插件最近一次 host_resp_alloc 记下的缓冲里。
    let (resp_ptr, resp_cap) = {
        let s = handle.store.data();
        (s.resp_ptr, s.resp_cap)
    };
    if n == 0 || resp_ptr <= 0 {
        return Ok(Vec::new());
    }
    // 不信任插件自报的长度:它可能声明 64 字节的缓冲却返回 2^31-1,让宿主
    // 从线性内存里拷出远超缓冲的字节。按声明的 cap 收窄(0 视作未设,仍按
    // 声明的长度读,由下面的内存边界兜底)。
    let n = if resp_cap > 0 { n.min(resp_cap) } else { n };
    let start = resp_ptr as usize;
    let end = start.checked_add(n as usize).context("响应地址溢出")?;
    let data = mem.data(&handle.store);
    let bytes = data.get(start..end).context("响应超出线性内存")?.to_vec();
    Ok(bytes)
}

/// 不超过 `max` 的最大字符边界偏移:截断必须落在边界上,否则切出的字节不是
/// 合法 UTF-8。detail 的带省略号截断与 host_kv_get 的前缀截断共用(一个加
/// 省略号一个不加,共用的是边界计算)。
///
/// 定义在契约 crate(`host_kv_get` 那一侧也要用它),这里只 re-export。
pub(super) use monitor_plugin_contract::host_linker::char_boundary_end;

/// 按字符边界截断,加省略号标记被截。
pub(super) fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    format!("{}…", &s[..char_boundary_end(s, max)])
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::log::render_log;
    use crate::plugin::registry::DEFAULT_TIMEOUT_MS;
    use crate::plugin::test_util::{app, compile, engine, expiry_event, row, MANIFEST, MINIMAL_WAT};

    /// 最小插件加载、编译、处理真实事件,都返回 0。两个事件类型都走一遍:
    /// 载荷形状不同(字段集不同),分配与写入路径相同。
    #[test]
    fn a_minimal_plugin_loads_and_handles_events() {
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(MINIMAL_WAT))).unwrap();
        assert_eq!(
            call_on_event(
                &engine,
                &app,
                &plugin,
                &expiry_event(),
                DEFAULT_FUEL_LIMIT,
                DEFAULT_TIMEOUT_MS,
                &new_log_sink(),
            )
            .unwrap(),
            0
        );
        let online = Event::AgentOnline { node_id: 5, name: "edge-1".into(), observed_at: 300 };
        assert_eq!(
            call_on_event(
                &engine,
                &app,
                &plugin,
                &online,
                DEFAULT_FUEL_LIMIT,
                DEFAULT_TIMEOUT_MS,
                &new_log_sink(),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn the_event_payload_reaches_the_plugin() {
        // on_event 把 (ptr, len) 复制到全局堆顶,再返回 len:Rust 侧从内存读回
        // 载荷,证明写入的地址与长度是插件实际收到的。
        let wat_text = r#"
(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $cap)))
    (local.get $ptr))
  (func (export "on_event") (param $ptr i32) (param $len i32) (result i32)
    (global.set $heap (i32.add (i32.const 16384) (local.get $len)))
    (memory.copy (i32.const 16384) (local.get $ptr) (local.get $len))
    (local.get $len)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let event = expiry_event();
        let n = call_on_event(
            &engine,
            &app,
            &plugin,
            &event,
            DEFAULT_FUEL_LIMIT,
            DEFAULT_TIMEOUT_MS,
            &new_log_sink(),
        )
        .unwrap();
        let json = serde_json::to_vec(&event).unwrap();
        assert_eq!(n as usize, json.len());
    }

    // ---- 模块加载(R6) ----

    #[test]
    fn corrupt_wasm_fails_to_load() {
        let engine = engine();
        let err = load(&engine, &row(b"\0asm\xde\xad\xbe\xef".to_vec())).unwrap_err();
        assert!(err.to_string().contains("编译失败"), "实际: {err}");
    }

    #[test]
    fn a_module_missing_exports_is_rejected() {
        let engine = engine();
        for (wat_text, needle) in [
            (
                r#"(module (memory (export "memory") 1)
                    (func (export "__alloc") (param i32) (result i32) (i32.const 0)))"#,
                "on_event",
            ),
            (
                r#"(module (memory (export "memory") 1)
                    (func (export "on_event") (param i32 i32) (result i32) (i32.const 0)))"#,
                "__alloc",
            ),
            (
                r#"(module (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
                    (func (export "__alloc") (param i32) (result i32) (i32.const 0)))"#,
                "memory",
            ),
        ] {
            let err = load(&engine, &row(compile(wat_text))).unwrap_err();
            assert!(err.to_string().contains(needle), "应报缺少 `{needle}`,实际: {err}");
        }
    }

    #[test]
    fn a_wrong_abi_version_fails_at_load() {
        let engine = engine();
        let mut r = row(compile(MINIMAL_WAT));
        r.manifest_json = MANIFEST.replace("abi_version = 2", "abi_version = 3");
        let err = load(&engine, &r).unwrap_err();
        // `{:#}` 展开错误链:load 包了一层 "manifest 无效",原因在下面。
        let whole = format!("{err:#}");
        assert!(whole.contains("abi_version"), "实际: {whole}");
    }

    // ---- fuel(KTD6) ----

    #[test]
    fn a_runaway_loop_is_cut_off_by_fuel() {
        let wat_text = r#"
(module
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "on_event") (param i32 i32) (result i32)
    (loop (br 0))
    (i32.const 0)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let err = call_on_event(
            &engine,
            &app,
            &plugin,
            &expiry_event(),
            10_000,
            DEFAULT_TIMEOUT_MS,
            &new_log_sink(),
        )
        .unwrap_err();
        // trap 信息在错误链深处,格式化整条链再找。
        let whole = format!("{err:#}");
        assert!(whole.to_lowercase().contains("fuel"), "应是 fuel 耗尽,实际: {whole}");
    }

    /// #3:宿主调用循环不能击穿超时沙箱。wasm 循环调 kv_get,每轮 wasm 指令
    /// 极少(给的 fuel 烧不完)、宿主侧也不耗时——没有 deadline 检查时它会一直
    /// 转到 fuel 尽头(远超 50ms 预算);有检查时 kv_get 在 deadline 后返回 -1,
    /// 循环当轮退出。参数都是合法的,-1 只可能来自预算检查。
    #[test]
    fn a_host_call_loop_is_cut_off_by_the_deadline() {
        let wat_text = r#"
(module
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "k")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (local $r i32)
    (loop $again
      ;; 无值返回 0:继续;预算耗尽返回 -1:退出。
      (local.set $r (call $kv_get (i32.const 1024) (i32.const 1) (i32.const 4096) (i32.const 64)))
      (br_if $again (i32.eqz (local.get $r))))
    (local.get $r)))"#;
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(wat_text))).unwrap();
        let started = std::time::Instant::now();
        // fuel 给到 50ms 内烧不完的量级;墙钟预算只有 50ms。
        let code = call_on_event(&engine, &app, &plugin, &expiry_event(), 1_000_000_000, 50, &new_log_sink())
            .unwrap();
        let elapsed = started.elapsed();
        assert_eq!(code, -1, "循环应以 kv_get 的预算耗尽返回码退出");
        assert!(elapsed < Duration::from_secs(5), "应在墙钟预算附近终止,实际 {elapsed:?}");
    }

    // ---- 插件日志汇集(A:失败原因的透出) ----

    /// 插件经 `host.log` 打的话要落进汇集点:面板上「为什么失败」全靠它。
    #[test]
    fn host_log_lines_land_in_the_sink() {
        let wat_text = r#"
(module
  (import "host" "log" (func $log (param i32 i32 i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "no bot_token")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $log (i32.const 2) (i32.const 1024) (i32.const 12))
    (i32.const 7)))"#;
        let engine = engine();
        let app = app();
        let module = wasmtime::Module::new(&engine, compile(wat_text)).unwrap();
        let logs = new_log_sink();
        let mut handle = instantiate(
            &engine,
            &app,
            "com.example.test",
            &module,
            DEFAULT_FUEL_LIMIT,
            DEFAULT_TIMEOUT_MS,
            &logs,
        )
        .unwrap();
        let on_event =
            handle.instance.get_typed_func::<(i32, i32), i32>(&mut handle.store, "on_event").unwrap();
        assert_eq!(on_event.call(&mut handle.store, (0, 0)).unwrap(), 7, "插件自己的返回码不受日志捕获影响");
        let detail = render_log(&logs, 500).expect("打过日志就该有 detail");
        assert_eq!(detail, "no bot_token");
    }
}

//! WASM plugin runtime.
//!
//! - [`Manifest::parse`] 校验 manifest(R7),
//! - [`load`] 把 db 行变成 [`LoadedPlugin`](R6 的前半段:编译 + 导出契约检查),
//! - [`call_on_event`] 每次事件新建 Store、注入宿主函数、以 fuel 限额调用插件,
//! - 6 个宿主函数(R8),
//! - [`Registry`] 启动预加载 enabled 插件(R10)、按 manifest.subscribes 派发
//!   (R5)、以超时/fuel 隔离每个插件(R9)、维护 dispatch_log 环形缓冲(R16)
//!   并回写 notification_log(R14)。
//!
//! # wasm 模块契约(U8 的示例插件按此实现)
//!
//! 模块必须导出:
//!
//! | 导出 | 签名 | 用途 |
//! |------|------|------|
//! | `memory` | 线性内存 | 宿主函数的指针都落在它上面 |
//! | `on_event` | `(ptr: i32, len: i32) -> i32` | 事件入口;入参指向 JSON 载荷,返回 0 表示成功,非 0 是插件自定义错误码 |
//! | `__alloc` | `(cap: i32) -> i32` | 分配器;宿主写载荷前通过它拿缓冲(`host_resp_alloc` 同样回调它) |
//!
//! 模块从 `"host"` 模块导入宿主函数(名字与返回值见各函数的文档;错误码统一为负数,
//! 成功时 kv_get/http_post 返回写入的字节数,其余返回 0)。
//!
//! 事件载荷是 [`crate::notification_bus::Event`] 的 JSON,形如
//! `{"type":"expiry_soon","node_id":7,...}`——按字段名反序列化、容忍新增字段。
//!
//! 资源模型(A8):引擎进程唯一(见 [`new_engine`]),`LoadedPlugin` 只缓存 manifest
//! 与 `Module`(均 Send+Sync);实例与 Store 每次调用重建——fuel 记在 Store 上,
//! 复用会让首次耗尽 fuel 的插件永久死亡,也无法并发调用。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use wasmtime::{Caller, Extern, Linker, Memory, Store};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::notification_bus::Event;
use crate::{db::PluginRow, App};

// ---------------------------------------------------------------------------
// Registry(U4):启动预加载、按 manifest.subscribes 的事件路由、dispatch_log
// 环形缓冲、notification_log 回写。
// ---------------------------------------------------------------------------

/// dispatch_log 的容量(KTD12)。环形:push_back 满了 pop_front,最近 1000 次
/// 派发结果始终可见,面板与测试都据此判断一次派发是否真的发生、结果如何。
pub const DISPATCH_LOG_CAP: usize = 1000;

/// `DispatchEntry::result` 取值 `success` 时表示插件返回 0:Rust 侧的比较与
/// 构造都引用这里,不散写魔法串。
pub const RESULT_SUCCESS: &str = "success";

/// fuel 限额的 setting key(KTD6)。每次派发都重读,改完下一次扫描即生效。
const SETTING_FUEL_LIMIT: &str = "plugin.fuel_limit";
/// 墙钟超时的 setting key(KTD6),单位毫秒。与 fuel 同读,理由相同。
const SETTING_TIMEOUT_MS: &str = "plugin.timeout_ms";
/// 超时缺省值:留出宿主开销后,略宽于插件 http 的 4 秒上限(见 [`HTTP_TIMEOUT`])。
const DEFAULT_TIMEOUT_MS: u64 = 5_000;

/// 回写 detail 的长度上限:面板单元格放得下,又足够诊断。
const DETAIL_MAX: usize = 500;

/// 一次 `on_event` 调用的审计记录。`result` 取值:
///
/// | 值 | 含义 |
/// |----|------|
/// | `success` | 插件返回 0 |
/// | `other:<错误码>` | 插件返回了自定义的非 0 错误码 |
/// | `timeout` | 超过 `plugin.timeout_ms` 的墙钟预算 |
/// | `fuel_exhausted` | trap 且错误链含 fuel(死循环被 KTD6 截断) |
/// | `host_error:<原因>` | 其余 trap / 实例化失败 / 任务崩溃 |
#[derive(Debug, Clone, Serialize)]
pub struct DispatchEntry {
    /// Unix 秒。
    pub at: i64,
    /// manifest 的 plugin_id,不是 db 行号:日志是给人看的。
    pub plugin_id: String,
    pub event_type: String,
    pub elapsed_ms: u64,
    pub result: String,
}

/// 插件注册表。`App.plugins` 持有它,它以 `Weak` 回指 `App`:强引用会组成
/// 循环(App → Registry → App),而两者都活到进程结束,循环的代价只是延迟
/// 释放;`Weak` 把这点代价也省了——App 在,upgrade 必成;App 拆了,派发本来
/// 就无事可做。
pub struct Registry {
    /// `App.engine` 的克隆(Engine 内部是 Arc,克隆廉价),与 App 共享同一
    /// 实例,插件 Module 的编译产物因此在两者间互通。
    engine: wasmtime::Engine,
    /// 见结构体文档:回指 App 的 `Weak`。由 [`Registry::init`] 在 App 进入
    /// Arc 之后写入——构造期还没有 Arc 可指。
    app: Weak<App>,
    /// key = plugin 表的行 id。enable/disable/remove 都以行 id 为准。
    loaded: HashMap<i64, LoadedPlugin>,
    /// 环形缓冲套一层 Arc:fire-and-forget 的汇总任务要在 dispatch 返回之后
    /// 继续写入,那时 `&self` 已不可借。
    dispatch_log: Arc<Mutex<VecDeque<DispatchEntry>>>,
    /// 已转发进 Registry 的事件数(不论有没有订阅者)。只有测试读它:
    /// notification_bus 的测试据此区分"记录了幂等行"和"真的转发了"。
    #[cfg(test)]
    dispatched: AtomicUsize,
}

impl Registry {
    /// `App::new` 里的占位构造:engine 就位、无插件、无回指。真正的初始化
    /// 在 App 被 Arc 包裹后由 [`Registry::init`] 完成。未 init 的 Registry
    /// 派发是 no-op——bus 的测试用不经 Arc 的 App 调 emit,依赖的正是这一点。
    pub fn empty(engine: wasmtime::Engine) -> Registry {
        Registry {
            engine,
            app: Weak::new(),
            loaded: HashMap::new(),
            dispatch_log: Arc::new(Mutex::new(VecDeque::new())),
            #[cfg(test)]
            dispatched: AtomicUsize::new(0),
        }
    }

    /// 启动预加载(KTD10):写入 `Weak` 回指,再把 enabled=1 的插件逐个装进
    /// 来。一个插件失败只影响它自己:状态落库为 `failed` 并带上原因,其余
    /// 插件照常加载。main 在 `Arc::new(App::new(..))` 之后调用一次。
    pub fn init(&mut self, app: &Arc<App>) {
        self.app = Arc::downgrade(app);
        let rows = match app.db.enabled_plugins() {
            Ok(rows) => rows,
            Err(e) => {
                warn!("读取插件列表失败,本次不加载任何插件: {e:#}");
                return;
            }
        };
        for row in rows {
            match load(&self.engine, &row) {
                Ok(plugin) => {
                    // 成功也落库:上一次启动若把它标成 failed,不清掉就永远
                    // 看不到它已经恢复。
                    if let Err(e) = app.db.set_plugin_status(row.id, "enabled", None) {
                        warn!(plugin = %row.plugin_id, "写插件状态失败: {e:#}");
                    }
                    info!(plugin = %row.plugin_id, "插件已加载");
                    self.loaded.insert(row.id, plugin);
                }
                Err(e) => {
                    warn!(plugin = %row.plugin_id, "插件加载失败,已停用: {e:#}");
                    let _ = app.db.set_plugin_status(row.id, "failed", Some(&format!("{e:#}")));
                }
            }
        }
    }

    /// 把一个已在 db 里 enabled 的插件装进内存(U5 的 enable API 在写完 db
    /// 后调用)。加载失败时把 `failed` 与原因落库并返回 Err,API 层转成 400;
    /// 成功时不再写库——API 层的 `set_plugin_enabled(id, true)` 在调用前已把
    /// status 置为 `enabled`、last_error 清空,与这里要写的完全相同,同一请求
    /// 写两次没有意义。
    pub fn enable_plugin(&mut self, app: &App, plugin_row_id: i64) -> Result<()> {
        let row =
            app.db.get_plugin(plugin_row_id)?.with_context(|| format!("插件 {plugin_row_id} 不存在"))?;
        match load(&self.engine, &row) {
            Ok(plugin) => {
                self.loaded.insert(plugin_row_id, plugin);
                Ok(())
            }
            Err(e) => {
                app.db.set_plugin_status(plugin_row_id, "failed", Some(&format!("{e:#}")))?;
                Err(e)
            }
        }
    }

    /// 移出内存即可;db 的 enabled 列由 API 层写,这里假定已经写完。
    /// 不在 loaded 里也照常返回:与"禁用一个加载失败的插件"是同一种无事可做。
    pub fn disable_plugin(&mut self, plugin_row_id: i64) {
        self.loaded.remove(&plugin_row_id);
    }

    /// 删除插件(U5 的 delete API):db 行由 API 层删,内存这边同步摘除。
    pub fn remove_plugin(&mut self, plugin_row_id: i64) {
        self.loaded.remove(&plugin_row_id);
    }

    pub fn is_loaded(&self, plugin_row_id: i64) -> bool {
        self.loaded.contains_key(&plugin_row_id)
    }

    #[cfg(test)]
    pub fn dispatch_count(&self) -> usize {
        self.dispatched.load(Ordering::Relaxed)
    }

    /// 派发日志快照,最新在前(新 → 旧):面板要的是"刚发生了什么",倒序让
    /// 最新条目在数组头部,前端不必再反转。
    pub fn dispatch_log_snapshot(&self) -> Vec<DispatchEntry> {
        self.dispatch_log.lock().unwrap_or_else(|e| e.into_inner()).iter().rev().cloned().collect()
    }

    /// 派发到所有订阅该事件的已启用插件(R5),fire-and-forget:emit 的调用
    /// 链是同步的,不等插件。
    ///
    /// 0 个订阅者不记 entry、不回写——dispatch_log 只记真的调用了插件的派发,
    /// 幂等行保持 success=0 恰好说明"没有插件在听"。
    ///
    /// 每个插件一个任务 + 外层 timeout(KTD8),全部完成后按 R14 回写
    /// notification_log。回写在一个汇总任务里做:dispatch 自己不 async,没有
    /// "等它们跑完"的自然位置,汇总任务补上这个位置。
    pub fn dispatch(&self, event: &Event) {
        #[cfg(test)]
        self.dispatched.fetch_add(1, Ordering::Relaxed);
        let subscribers: Vec<LoadedPlugin> = self
            .loaded
            .values()
            .filter(|p| p.manifest.subscribes.iter().any(|s| s == event.type_name()))
            .cloned()
            .collect();
        if subscribers.is_empty() {
            return;
        }
        let Some(app) = self.app.upgrade() else { return };
        // spawn 需要运行时上下文;emit 可能在任何线程被调。没有运行时就没有
        // 派发,记一条而不是 panic。
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("插件派发需要 tokio 运行时,当前线程没有;{} 事件被丢弃", event.type_name());
            return;
        };
        let fuel = setting_u64(&app, SETTING_FUEL_LIMIT, DEFAULT_FUEL_LIMIT);
        let timeout_ms = setting_u64(&app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        let engine = self.engine.clone();
        let log = self.dispatch_log.clone();
        let event = event.clone();
        handle.spawn(async move {
            let mut handles = Vec::with_capacity(subscribers.len());
            for plugin in subscribers {
                // id 抄一份:run_one 拿走了 plugin 本体,join 失败(任务崩溃)时
                // 合成 entry 还需要它。
                let plugin_id = plugin.manifest.plugin_id.clone();
                let task = run_one(engine.clone(), app.clone(), plugin, event.clone(), fuel, timeout_ms);
                handles.push((plugin_id, tokio::spawn(task)));
            }
            let mut entries = Vec::with_capacity(handles.len());
            for (plugin_id, h) in handles {
                match h.await {
                    Ok(entry) => entries.push(entry),
                    Err(join) => entries.push(DispatchEntry {
                        at: Utc::now().timestamp(),
                        plugin_id,
                        event_type: event.type_name().into(),
                        elapsed_ms: 0,
                        result: format!("host_error:{join}"),
                    }),
                }
            }
            for entry in &entries {
                push_entry(&log, entry.clone());
            }
            write_back(&app, &event, &entries);
        });
    }

    /// 单插件派发一次并等待结果,U5 的"测试通知"接口。与 [`Registry::dispatch`]
    /// 走同一条执行路径(含超时与 fuel),但不写 notification_log:合成事件不
    /// 占幂等键,真实事件的成功与否不该被一次手工测试覆盖。
    pub async fn dispatch_one(&self, plugin_row_id: i64, event: &Event) -> Result<DispatchEntry> {
        let Some(plugin) = self.loaded.get(&plugin_row_id) else {
            bail!("插件 {plugin_row_id} 未加载");
        };
        let plugin = plugin.clone();
        let Some(app) = self.app.upgrade() else {
            bail!("App 已拆除,无法派发");
        };
        let fuel = setting_u64(&app, SETTING_FUEL_LIMIT, DEFAULT_FUEL_LIMIT);
        let timeout_ms = setting_u64(&app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        let entry = run_one(self.engine.clone(), app, plugin, event.clone(), fuel, timeout_ms).await;
        push_entry(&self.dispatch_log, entry.clone());
        Ok(entry)
    }
}

/// 单个插件处理一个事件,产出审计 entry。超时与 fuel 双重隔离(KTD8/KTD6):
///
/// wasm 执行放在 block_in_place 里——它把执行从调度线程挪开,又保留运行时
/// 上下文(host_http_post 要 `Handle::try_current`);但 block_in_place 是
/// 同步阻塞,直接在本任务里调用会让外层的 timeout 永远等不到 poll。所以再
/// spawn 一层,让本任务只在 worker 上等 JoinHandle,计时由别的 worker 完成。
/// 超时后那个任务继续烧到 fuel 尽头——纯 wasm 死循环由 fuel 硬兜底;宿主调用
/// 循环(fuel 不计量宿主侧执行)由 PluginState.deadline 拦截,见 [`call_on_event`]。
async fn run_one(
    engine: wasmtime::Engine,
    app: Arc<App>,
    plugin: LoadedPlugin,
    event: Event,
    fuel_limit: u64,
    timeout_ms: u64,
) -> DispatchEntry {
    let started = std::time::Instant::now();
    let plugin_id = plugin.manifest.plugin_id.clone();
    let event_type = event.type_name();
    let task = tokio::spawn(async move {
        tokio::task::block_in_place(move || {
            call_on_event(&engine, &app, &plugin, &event, fuel_limit, timeout_ms)
        })
    });
    let result = match tokio::time::timeout(Duration::from_millis(timeout_ms), task).await {
        Err(_) => "timeout".to_owned(),
        Ok(Ok(Ok(0))) => RESULT_SUCCESS.to_owned(),
        Ok(Ok(Ok(code))) => format!("other:{code}"),
        Ok(Ok(Err(e))) => {
            // trap 的具体信息在错误链深处,格式化整条链再判 fuel。
            let whole = format!("{e:#}");
            if whole.to_lowercase().contains("fuel") {
                "fuel_exhausted".to_owned()
            } else {
                format!("host_error:{}", truncate(&whole, DETAIL_MAX))
            }
        }
        Ok(Err(join)) => format!("host_error:{join}"),
    };
    DispatchEntry {
        at: Utc::now().timestamp(),
        plugin_id,
        event_type: event_type.into(),
        elapsed_ms: started.elapsed().as_millis() as u64,
        result,
    }
}

/// 记一条派发结果,环形淘汰最旧(KTD12)。
fn push_entry(log: &Arc<Mutex<VecDeque<DispatchEntry>>>, entry: DispatchEntry) {
    let mut log = log.lock().unwrap_or_else(|e| e.into_inner());
    log.push_back(entry);
    while log.len() > DISPATCH_LOG_CAP {
        log.pop_front();
    }
}

/// R14 回写:所有订阅插件都成功才置 success=1,否则保持 0 并点名失败者。
/// ExpirySoon 行由 emit 的 record_dispatch 先立好,状态事件行由
/// transition_state_event 立好,这里只做 UPDATE;行不存在(比如测试直接调
/// dispatch)时 UPDATE 落空,无害。
fn write_back(app: &App, event: &Event, entries: &[DispatchEntry]) {
    let success = entries.iter().all(|e| e.result == RESULT_SUCCESS);
    let detail = if success {
        String::new()
    } else {
        let failed: Vec<String> = entries
            .iter()
            .filter(|e| e.result != RESULT_SUCCESS)
            .map(|e| format!("{}: {}", e.plugin_id, e.result))
            .collect();
        truncate(&failed.join("; "), DETAIL_MAX)
    };
    if let Err(e) = app.db.mark_dispatch_result(
        event.node_id(),
        event.type_name(),
        event.threshold_or_state_key(),
        success,
        &detail,
    ) {
        warn!("回写派发结果失败: {e:#}");
    }
}

/// 读一个 u64 setting,缺省或非法值回落默认——一个手滑的 `abc` 不该把派发
/// 整个停掉。
fn setting_u64(app: &App, key: &str, default: u64) -> u64 {
    app.db.get(key).and_then(|v| v.trim().parse().ok()).unwrap_or(default)
}

/// 不超过 `max` 的最大字符边界偏移:截断必须落在边界上,否则切出的字节不是
/// 合法 UTF-8。detail 的带省略号截断与 host_kv_get 的前缀截断共用(一个加
/// 省略号一个不加,共用的是边界计算)。
fn char_boundary_end(s: &str, max: usize) -> usize {
    let mut end = s.len().min(max);
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    end
}

/// 按字符边界截断,加省略号标记被截。
fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_owned();
    }
    format!("{}…", &s[..char_boundary_end(s, max)])
}

// ---------------------------------------------------------------------------
// Manifest(R7)
// ---------------------------------------------------------------------------

/// 宿主与插件之间的 ABI 版本。宿主大版本升级时递增;不匹配的插件在加载时被拒。
pub const ABI_VERSION: i64 = 1;

/// v1 的事件词表,单一来源是 [`Event::KNOWN`]:manifest 校验、db 的状态行
/// 与扫描循环读的都是同一组名字。manifest 声明订阅未来才有的事件名会在这里
/// 被拒:静默接受会让拼写错误无声失效,显式契约尽早暴露错误(KTD2)。
pub const KNOWN_EVENT_NAMES: [&str; 3] = Event::KNOWN;

/// plugin.toml。字段与校验规则见 [`Manifest::parse`]。
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    /// 插件的稳定标识,反向域风格(如 `com.example.mailer`)。非空、不含 ':'
    /// ——它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符。
    pub plugin_id: String,
    /// 面板里显示的名字。
    pub name: String,
    /// 语义化版本。v1 只做非空校验。
    pub version: String,
    /// 必须等于 [`ABI_VERSION`]。
    pub abi_version: i64,
    /// 订阅的事件名,必须是 [`KNOWN_EVENT_NAMES`] 之一,且至少一项——不订阅任何
    /// 事件的插件永远不会被派发,上传时拒绝而不是装一个死插件。
    pub subscribes: Vec<String>,
    /// 包内 wasm 入口文件名。上传 API(U5)按它从包里取模块;运行期不再使用。
    #[serde(default = "default_wasm_entry")]
    pub wasm_entry: String,
}

fn default_wasm_entry() -> String {
    "plugin.wasm".into()
}

impl Manifest {
    /// 解析并校验 manifest 文本。失败返回带原因的错误——上传 API(U5)把它转成
    /// 400,所以每条消息都要让插件作者知道改哪里。
    pub fn parse(toml_text: &str) -> Result<Self> {
        let m: Manifest = toml::from_str(toml_text).context("manifest 不是合法的 TOML")?;
        if m.plugin_id.trim().is_empty() {
            bail!("manifest.plugin_id 不能为空");
        }
        if m.plugin_id.contains(':') {
            bail!("manifest.plugin_id 不能包含 ':'(它是 kv 命名空间的分隔符)");
        }
        if m.name.trim().is_empty() {
            bail!("manifest.name 不能为空");
        }
        if m.version.trim().is_empty() {
            bail!("manifest.version 不能为空");
        }
        if m.abi_version != ABI_VERSION {
            bail!(
                "manifest.abi_version 必须为 {ABI_VERSION}(当前 {}),请用匹配的插件 SDK 重build",
                m.abi_version
            );
        }
        if m.subscribes.is_empty() {
            bail!("manifest.subscribes 至少要订阅一个事件");
        }
        for event in &m.subscribes {
            if !KNOWN_EVENT_NAMES.contains(&event.as_str()) {
                bail!(
                    "manifest.subscribes 含未知事件 `{event}`;v1 支持的事件: {}",
                    KNOWN_EVENT_NAMES.join(", ")
                );
            }
        }
        Ok(m)
    }
}

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
    // "模块长得不对"这种配置型错误。
    use wasmtime::ExternType;
    if !matches!(module.get_export("memory"), Some(ExternType::Memory(_))) {
        bail!("插件 {} 的模块缺少导出 `memory`", manifest.plugin_id);
    }
    for name in ["on_event", "__alloc"] {
        if !matches!(module.get_export(name), Some(ExternType::Func(_))) {
            bail!("插件 {} 的模块缺少导出 `{name}`", manifest.plugin_id);
        }
    }
    Ok(LoadedPlugin { manifest, module })
}

// ---------------------------------------------------------------------------
// 宿主函数(R8)
// ---------------------------------------------------------------------------

/// 每次调用的默认 fuel 限额(KTD6)。dispatch 每次读 setting
/// `plugin.fuel_limit` 覆写,缺省回落到这里。
pub const DEFAULT_FUEL_LIMIT: u64 = 1_000_000;

/// 单个插件的 kv 值上限:8 KiB,足够放渠道配置,又不会让 setting 表被一个插件
/// 当对象存储用。面板的 kv 写入引用同一个上限:「面板能写的不能比插件运行时
/// 能写的多」,否则面板成了绕过插件存储限额的后门。
pub const KV_VALUE_MAX: usize = 8 * 1024;

/// 插件 http 请求的墙钟超时(A13):4 秒,落在 5 秒的派发预算内,留 1 秒给宿主
/// 自己的开销。挂在请求上而不是再建一个 client:全局 `app.http` 的 15 秒超时
/// 服务于面板自身的下载,不能为插件收短。
const HTTP_TIMEOUT: Duration = Duration::from_secs(4);

/// 单次 http 响应体的硬上限:64 KiB。插件声明的 resp_cap 再大也读这么多——
/// 有界下载要防的正是"cap 被声明成超大值/响应体本身无限大"的内存放大。
const HTTP_RESP_MAX: usize = 64 * 1024;

/// Store 的 user data:宿主函数看得见的全部宿主侧状态。
pub(crate) struct PluginState {
    /// kv 命名空间隔离:`plugin.<plugin_id>:<key>`。
    plugin_id: String,
    /// 宿主回程:kv 与 http 都要它。App 本就活在 Arc 里,克隆只是引用计数。
    app: Arc<App>,
    /// 本次 `on_event` 的墙钟预算截止点(由 `instantiate` 从 timeout_ms 折算)。
    /// 外层 tokio 超时只能 detach 后台任务,fuel 又只计量 wasm 指令——宿主调用
    /// 循环(每轮 wasm 指令极少、宿主侧执行再久也不耗 fuel)只有这道检查能拦。
    deadline: std::time::Instant,
    /// 最近一次 `host_resp_alloc` 拿到的缓冲。`host_http_post` 的 resp_ptr 传 0
    /// 时回落到这里,插件可以少传两个参数。
    resp_ptr: i32,
    resp_cap: i32,
}

/// 宿主函数的返回错误码。成功:kv_get/http_post 返回写入字节数,其余返回 0。
///
/// | 码 | 含义 |
/// |----|------|
/// | -1 | 内存越界 / 非法 UTF-8 / 值超限 / `__alloc` 缺失或失败 |
/// | -2 | http:URL 不是 `https://`(或 kv:数据库写入失败) |
/// | -3 | http:method 不是 `POST` |
/// | -4 | http:网络请求失败 / 宿主不在异步运行时上下文 / 墙钟预算耗尽 |
/// | -5 | http:响应状态非 2xx |
const ERR_BOUNDS: i32 = -1;

/// 从插件线性内存读 `[ptr, ptr+len)`。返回 `None` 表示越界。宿主函数绝不能
/// panic(会把整个进程带走),所以一切访问都从这里走、先检查后拷贝。
fn read_mem(caller: &mut Caller<'_, PluginState>, ptr: i32, len: i32) -> Option<Vec<u8>> {
    if ptr < 0 || len < 0 {
        return None;
    }
    let mem = caller.get_export("memory")?.into_memory()?;
    let data = mem.data(&*caller);
    let start = ptr as usize;
    let end = start.checked_add(len as usize)?;
    Some(data.get(start..end)?.to_vec())
}

/// 读一段必须合法 UTF-8 的文本(key、method、URL)。
fn read_text(caller: &mut Caller<'_, PluginState>, ptr: i32, len: i32) -> Option<String> {
    let bytes = read_mem(caller, ptr, len)?;
    String::from_utf8(bytes).ok()
}

/// 往插件线性内存写字节。越界返回 false。
fn write_mem(caller: &mut Caller<'_, PluginState>, ptr: i32, bytes: &[u8]) -> bool {
    if ptr < 0 {
        return false;
    }
    let Some(mem) = caller.get_export("memory").and_then(Extern::into_memory) else {
        return false;
    };
    let start = ptr as usize;
    let Some(end) = start.checked_add(bytes.len()) else {
        return false;
    };
    let data = mem.data_mut(&mut *caller);
    let Some(target) = data.get_mut(start..end) else {
        return false;
    };
    target.copy_from_slice(bytes);
    true
}

/// 把调用侧的 Store/内存组合暴露给 tests:测试需要直接看内存与 db,而
/// `call_on_event` 只返回 i32。
pub(crate) struct InstanceHandle {
    pub store: Store<PluginState>,
    pub instance: wasmtime::Instance,
}

/// http 响应的写回计划(纯函数,便于单测):决定写到哪个缓冲、最多写多少字节。
///
/// - 选缓冲:显式 `resp_ptr > 0` 优先,否则回落最近一次 `host_resp_alloc`
///   (传入 `last`);都不可用返回 `None`——请求已发出,宿主返回 0 字节而不是
///   报错。
/// - 有效容量 = `min(声明 cap, HTTP_RESP_MAX)`:既是有界下载的上限(读到
///   即停,超出的字节丢弃——与"整读后截断"同效,但不会把大响应体整个拉进
///   内存),也是最终写回的截断长度。`bytes_len` 传 `usize::MAX` 时返回的
///   第二项就是纯容量,下载前据此定界。
fn resp_write_plan(resp_ptr: i32, resp_cap: i32, last: (i32, i32), bytes_len: usize) -> Option<(i32, usize)> {
    let (ptr, cap) = if resp_ptr > 0 { (resp_ptr, resp_cap) } else { last };
    if ptr <= 0 || cap < 0 {
        return None;
    }
    let cap = (cap as usize).min(HTTP_RESP_MAX);
    Some((ptr, bytes_len.min(cap)))
}

/// 新建 Store(带 fuel 与墙钟 deadline)、注册宿主函数、实例化。`call_on_event`
/// 的骨架,也是测试直接驱动单个宿主函数的入口。
pub(crate) fn instantiate(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin_id: &str,
    module: &wasmtime::Module,
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<InstanceHandle> {
    let mut store = Store::new(
        engine,
        PluginState {
            plugin_id: plugin_id.into(),
            app: app.clone(),
            deadline: std::time::Instant::now() + Duration::from_millis(timeout_ms),
            resp_ptr: 0,
            resp_cap: 0,
        },
    );
    // fuel 在任何 wasm 执行(含 start 段)之前就位。
    store.set_fuel(fuel_limit)?;
    let linker = host_linker(engine)?;
    let instance = linker.instantiate(&mut store, module)?;
    Ok(InstanceHandle { store, instance })
}

/// 注册 6 个宿主函数(R8)。每次实例化都重建:Linker 不能跨 Store 复用已定义
/// 的 Func,重建的开销微秒级,正确性优先。
fn host_linker(engine: &wasmtime::Engine) -> Result<Linker<PluginState>> {
    let mut linker: Linker<PluginState> = Linker::new(engine);

    // host_log(level, ptr, len):0=debug 1=info 2=warn 3=error。越界或非法 UTF-8
    // 时记一条宿主侧 warn、内容按空处理——日志不能让插件把进程带崩。
    linker.func_wrap(
        "host",
        "log",
        |mut caller: Caller<'_, PluginState>, level: i32, ptr: i32, len: i32| {
            let plugin_id = caller.data().plugin_id.clone();
            let text = match read_text(&mut caller, ptr, len) {
                Some(text) => text,
                None => {
                    warn!(plugin = %plugin_id, "host_log 越界或非法 UTF-8: ptr={ptr} len={len}");
                    String::new()
                }
            };
            match level {
                0 => tracing::debug!(plugin = %plugin_id, "{text}"),
                1 => tracing::info!(plugin = %plugin_id, "{text}"),
                2 => warn!(plugin = %plugin_id, "{text}"),
                _ => {
                    tracing::error!(plugin = %plugin_id, "{text}");
                }
            }
        },
    )?;

    // host_now() -> i64:Unix 秒。
    linker.func_wrap("host", "now", || -> i64 { Utc::now().timestamp() })?;

    // host_kv_get(key_ptr, key_len, out_ptr, out_cap) -> i32:
    //   >0 写入 out 的字节数;0 无值;-1 越界/非法 UTF-8。值超 out_cap 时在
    //   UTF-8 字符边界截断——插件拿到的是前缀,长度对得上。
    linker.func_wrap(
        "host",
        "kv_get",
        |mut caller: Caller<'_, PluginState>,
         key_ptr: i32,
         key_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> i32 {
            // 墙钟预算检查(#3):kv 词表没有专门的预算错误码,复用 -1(负数让
            // 插件能感知预算耗尽并退出循环;正常路径不会与越界混淆——越界是
            // 参数问题,预算是时间问题,都会让插件放弃本次调用)。
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %caller.data().plugin_id, "kv_get 超出派发墙钟预算,拒绝");
                return ERR_BOUNDS;
            }
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            let Some(value) = caller.data().app.db.get(&format!("plugin.{plugin_id}:{key}")) else {
                return 0;
            };
            if out_ptr < 0 || out_cap < 0 {
                return ERR_BOUNDS;
            }
            let cap = out_cap as usize;
            let end = char_boundary_end(&value, cap);
            let bytes = &value.as_bytes()[..end];
            if !write_mem(&mut caller, out_ptr, bytes) {
                return ERR_BOUNDS;
            }
            bytes.len() as i32
        },
    )?;

    // host_kv_set(key_ptr, key_len, val_ptr, val_len) -> i32:
    //   0 成功;-1 越界/非法 UTF-8/值超 8 KiB;-2 数据库失败。
    linker.func_wrap(
        "host",
        "kv_set",
        |mut caller: Caller<'_, PluginState>, key_ptr: i32, key_len: i32, val_ptr: i32, val_len: i32| -> i32 {
            let Some(key) = read_text(&mut caller, key_ptr, key_len) else {
                return ERR_BOUNDS;
            };
            let Some(value) = read_text(&mut caller, val_ptr, val_len) else {
                return ERR_BOUNDS;
            };
            if value.len() > KV_VALUE_MAX {
                warn!(plugin = %caller.data().plugin_id, key = %key, "host_kv_set 值超过 {} 字节上限", KV_VALUE_MAX);
                return ERR_BOUNDS;
            }
            let plugin_id = caller.data().plugin_id.clone();
            let namespaced = format!("plugin.{plugin_id}:{key}");
            if caller.data().app.db.set(&namespaced, &value).is_err() {
                return -2;
            }
            0
        },
    )?;

    // host_resp_alloc(cap) -> i32:宿主无法直接在 wasm 堆上分配,采用生态标准的
    // allocator 回环——回调模块自己导出的 `__alloc(cap) -> ptr`,把指针记入
    // PluginState 并返回给插件。模块未导出 `__alloc`(或分配失败/返回非正指针)
    // 时返回 -1;调用方因此要导出它,模块契约(见模块文档)在加载时已检查。
    linker.func_wrap("host", "resp_alloc", |mut caller: Caller<'_, PluginState>, cap: i32| -> i32 {
        if cap <= 0 {
            return ERR_BOUNDS;
        }
        let Some(func) = caller.get_export("__alloc").and_then(Extern::into_func) else {
            warn!(plugin = %caller.data().plugin_id, "host_resp_alloc 找不到 __alloc 导出");
            return ERR_BOUNDS;
        };
        let Ok(typed) = func.typed::<(i32,), i32>(&caller) else {
            return ERR_BOUNDS;
        };
        match typed.call(&mut caller, (cap,)) {
            Ok(ptr) if ptr > 0 => {
                let state = caller.data_mut();
                state.resp_ptr = ptr;
                state.resp_cap = cap;
                ptr
            }
            _ => ERR_BOUNDS,
        }
    })?;

    // host_http_post(method_ptr, method_len, url_ptr, url_len, body_ptr, body_len,
    //                resp_ptr, resp_cap) -> i32:
    //   >0 写入 resp 的字节数;-1 参数越界/非法 UTF-8/resp 写不进;
    //   -2 URL 非 https;-3 method 非 POST;-4 网络失败/预算耗尽;-5 状态非 2xx。
    // v1 固定发 `Content-Type: application/json` 的 POST(webhook 事实标准)。
    // 日志只记 method 与 host:URL 可能内嵌 bot token。
    linker.func_wrap(
        "host",
        "http_post",
        |mut caller: Caller<'_, PluginState>,
         method_ptr: i32,
         method_len: i32,
         url_ptr: i32,
         url_len: i32,
         body_ptr: i32,
         body_len: i32,
         resp_ptr: i32,
         resp_cap: i32|
         -> i32 {
            let Some(method) = read_text(&mut caller, method_ptr, method_len) else {
                return ERR_BOUNDS;
            };
            let Some(url) = read_text(&mut caller, url_ptr, url_len) else {
                return ERR_BOUNDS;
            };
            let Some(body) = read_mem(&mut caller, body_ptr, body_len) else {
                return ERR_BOUNDS;
            };
            let plugin_id = caller.data().plugin_id.clone();
            // 墙钟预算检查(#3):fuel 不计量宿主侧执行,超时也只 detach 任务;
            // 入口拒绝让 wasm 侧的调用循环每轮拿到 -4,配合 fuel 兜底终止循环。
            // -4 沿用"网络失败"码,语义是"本次请求不发出:预算耗尽"。
            if std::time::Instant::now() >= caller.data().deadline {
                warn!(plugin = %plugin_id, "host_http_post 超出派发墙钟预算,拒绝");
                return -4;
            }
            if method != "POST" {
                warn!(plugin = %plugin_id, method = %method, "host_http_post v1 只接受 POST");
                return -3;
            }
            if !url.starts_with("https://") {
                warn!(plugin = %plugin_id, "host_http_post 拒绝非 https URL");
                return -2;
            }
            // 只记 host,不记完整 URL:路径与查询串可能带 token。
            let host = url.split('/').nth(2).unwrap_or_default().to_string();
            let app = caller.data().app.clone();
            // 下载前先定缓冲与有效容量:容量同时约束下载(读到即停)与写回截断。
            // 没有可用缓冲也按硬上限有界下载——读完丢弃,不能不设界。
            let last = {
                let state = caller.data();
                (state.resp_ptr, state.resp_cap)
            };
            let download_cap = resp_write_plan(resp_ptr, resp_cap, last, usize::MAX)
                .map(|(_, cap)| cap)
                .unwrap_or(HTTP_RESP_MAX);
            let Some(handle) = tokio::runtime::Handle::try_current().ok() else {
                warn!(plugin = %plugin_id, "host_http_post 不在异步运行时上下文中");
                return -4;
            };
            let request = app
                .http
                .post(&url)
                .timeout(HTTP_TIMEOUT)
                .header("content-type", "application/json")
                .body(body);
            let outcome = handle.block_on(async {
                let mut resp = request.send().await?;
                let status = resp.status();
                // 有界读取(#4):按 chunk 累计到 download_cap 即停,超出的字节
                // 丢弃——截断语义与"整读后截断"一致,但大响应体不再整体进内存。
                let mut buf: Vec<u8> = Vec::new();
                while buf.len() < download_cap {
                    match resp.chunk().await {
                        Ok(Some(chunk)) => {
                            let remaining = download_cap - buf.len();
                            buf.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
                        }
                        Ok(None) => break,
                        Err(err) => return Err(err),
                    }
                }
                Ok::<_, reqwest::Error>((status, buf))
            });
            let (status, bytes) = match outcome {
                Ok(ok) => ok,
                Err(err) => {
                    warn!(plugin = %plugin_id, host = %host, error = %err, "host_http_post 请求失败");
                    return -4;
                }
            };
            if !status.is_success() {
                warn!(plugin = %plugin_id, host = %host, status = %status, "host_http_post 非 2xx 响应");
                return -5;
            }
            // resp_ptr 为 0 时回落到最近一次 host_resp_alloc 的缓冲;没有可用
            // 缓冲但请求已发出:不报错,返回 0 字节。选缓冲与截断见 resp_write_plan。
            let Some((ptr, n)) = resp_write_plan(resp_ptr, resp_cap, last, bytes.len()) else {
                return 0;
            };
            if !write_mem(&mut caller, ptr, &bytes[..n]) {
                return ERR_BOUNDS;
            }
            n as i32
        },
    )?;

    Ok(linker)
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
pub fn call_on_event(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    plugin: &LoadedPlugin,
    event: &Event,
    fuel_limit: u64,
    timeout_ms: u64,
) -> Result<i32> {
    let mut handle =
        instantiate(engine, app, &plugin.manifest.plugin_id, &plugin.module, fuel_limit, timeout_ms)?;
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

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

/// 最小合法模块:memory + bump 分配器 + 恒返回 0 的 on_event。api.rs 的上传
/// 测试与本模块的测试共用同一份,fixture 漂移会让两边测的不是同一个东西。
#[cfg(test)]
pub(crate) const MINIMAL_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $cap)))
    (local.get $ptr))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0)))"#;

#[cfg(test)]
// `dispatch_one` is awaited through a read guard of the plugin registry in
// single-tenant tests: each test owns its `App`, the guard can contend with
// nothing, and restructuring to hand the registry out of the lock would test a
// different shape than production uses. The production path (`api::test_plugin`)
// keeps the guard off the await via `spawn_blocking`.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::db::Db;

    /// 通过 manifest 校验的标准测试 manifest。
    const MANIFEST: &str = r#"
plugin_id = "com.example.test"
name = "Test Plugin"
version = "1.0.0"
abi_version = 1
subscribes = ["expiry_soon", "agent_offline"]
"#;

    fn app() -> Arc<App> {
        Arc::new(App::for_test(Db::open(":memory:").unwrap()))
    }

    fn row(wasm: Vec<u8>) -> PluginRow {
        PluginRow {
            id: 1,
            plugin_id: "com.example.test".into(),
            name: "Test".into(),
            version: "1.0.0".into(),
            manifest_json: MANIFEST.into(),
            wasm_blob: wasm,
            wasm_sha256: String::new(),
            enabled: true,
            status: "enabled".into(),
            last_error: None,
            uploaded_at: 0,
        }
    }

    fn engine() -> wasmtime::Engine {
        new_engine()
    }

    fn expiry_event() -> Event {
        Event::ExpirySoon {
            node_id: 7,
            name: "edge-1".into(),
            expires_at: "2026-10-01".into(),
            days_left: 7,
            threshold_days: 7,
        }
    }

    fn compile(wat_text: &str) -> Vec<u8> {
        wat::parse_str(wat_text).unwrap()
    }

    /// 最小插件加载、编译、处理真实事件,都返回 0。两个事件类型都走一遍:
    /// 载荷形状不同(字段集不同),分配与写入路径相同。
    #[test]
    fn a_minimal_plugin_loads_and_handles_events() {
        let engine = engine();
        let app = app();
        let plugin = load(&engine, &row(compile(MINIMAL_WAT))).unwrap();
        assert_eq!(
            call_on_event(&engine, &app, &plugin, &expiry_event(), DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS)
                .unwrap(),
            0
        );
        let online = Event::AgentOnline { node_id: 5, name: "edge-1".into(), observed_at: 300 };
        assert_eq!(
            call_on_event(&engine, &app, &plugin, &online, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap(),
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
        let n =
            call_on_event(&engine, &app, &plugin, &event, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap();
        let json = serde_json::to_vec(&event).unwrap();
        assert_eq!(n as usize, json.len());
    }

    // ---- manifest 校验(R7) ----

    #[test]
    fn a_valid_manifest_parses() {
        let m = Manifest::parse(MANIFEST).unwrap();
        assert_eq!(m.plugin_id, "com.example.test");
        assert_eq!(m.subscribes, ["expiry_soon", "agent_offline"]);
        assert_eq!(m.wasm_entry, "plugin.wasm", "缺省的 wasm_entry");
        // 逐字段重写为非法值,每条都应带明确原因被拒;空串是控制组。
        for (field, value, needle) in [
            ("abi_version", "2", "abi_version"),
            ("plugin_id", "\"a:b\"", "':'"),
            ("plugin_id", "\"\"", "不能为空"),
            ("version", "\"\"", "不能为空"),
            ("subscribes", "[]", "至少"),
            ("subscribes", "[\"expiryy_soon\"]", "未知事件"),
        ] {
            let edited = MANIFEST
                .lines()
                .map(|line| if line.starts_with(field) { format!("{field} = {value}") } else { line.into() })
                .collect::<Vec<_>>()
                .join("\n");
            let err = Manifest::parse(&edited).unwrap_err().to_string();
            assert!(err.contains(needle), "把 `{field}` 改成 {value} 应报 `{needle}`,实际: {err}");
        }
    }

    #[test]
    fn a_broken_manifest_is_not_toml() {
        assert!(Manifest::parse("plugin_id = ").is_err());
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
        r.manifest_json = MANIFEST.replace("abi_version = 1", "abi_version = 3");
        let err = load(&engine, &r).unwrap_err();
        // `{:#}` 展开错误链:load 包了一层 "manifest 无效",原因在下面。
        let whole = format!("{err:#}");
        assert!(whole.contains("abi_version"), "实际: {whole}");
    }

    // ---- 宿主函数(R8) ----

    /// 实例化一个 WAT 模块并保留 Store:测试要直接读内存与 db。
    fn spawn(engine: &wasmtime::Engine, app: &Arc<App>, wat_text: &str) -> InstanceHandle {
        let module = wasmtime::Module::new(engine, compile(wat_text)).unwrap();
        instantiate(engine, app, "com.example.test", &module, DEFAULT_FUEL_LIMIT, DEFAULT_TIMEOUT_MS).unwrap()
    }

    /// 通过 on_event 驱动:kv_set 写入再 kv_get 读出到固定地址,返回读到的字节数。
    /// 同时调 host_log 与 host_now,证明它们可被调用。
    const KV_WAT: &str = r#"
(module
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (import "host" "log" (func $log (param i32 i32 i32)))
  (import "host" "now" (func $now (result i64)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "mykey")
  (data (i32.const 2048) "myvalue")
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (i32.const 8192))
    (i32.add (local.get $ptr) (local.get $cap)))
  (func (export "on_event") (param i32 i32) (result i32)
    (local $now i64)
    (local.set $now (call $now))
    (call $log (i32.const 1) (i32.const 2048) (i32.const 7))
    (drop (call $kv_set (i32.const 1024) (i32.const 5) (i32.const 2048) (i32.const 7)))
    (call $kv_get (i32.const 1024) (i32.const 5) (i32.const 4096) (i32.const 64))))"#;

    #[test]
    fn kv_round_trips_through_the_setting_table() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, KV_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        let n = on_event.call(&mut h.store, (0, 0)).unwrap();
        assert_eq!(n, 7, "kv_get 应写回 7 个字节");
        // 内容从插件内存读回:kv_get 写在 4096。
        let mem = h.instance.get_memory(&mut h.store, "memory").unwrap();
        assert_eq!(&mem.data(&h.store)[4096..4103], b"myvalue");
        // 落库形式是带命名空间的 key,两个插件不会互相覆盖。
        assert_eq!(app.db.get("plugin.com.example.test:mykey").as_deref(), Some("myvalue"));
        assert_eq!(app.db.get("plugin.other:mykey"), None, "命名空间隔离");
    }

    /// host_log 的越界与非法 UTF-8、kv_get 的越界 out_ptr:返回错误码而不是 panic。
    /// kv 的 key 必须先在 setting 表里有值,kv_get 才会走到写内存的越界检查。
    const OOB_WAT: &str = r#"
(module
  (import "host" "log" (func $log (param i32 i32 i32)))
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (import "host" "kv_get" (func $kv_get (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "k")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    ;; 越界日志:ptr 指向内存之外。
    (call $log (i32.const 3) (i32.const 268435456) (i32.const 100))
    ;; 非法 UTF-8:0xff 开头。
    (call $log (i32.const 3) (i32.const 8192) (i32.const 4))
    ;; 先写入一个值,让下面的 kv_get 走到写内存这一步。
    (drop (call $kv_set (i32.const 1024) (i32.const 1) (i32.const 1024) (i32.const 1)))
    ;; out_ptr 越界:-1。
    (call $kv_get (i32.const 1024) (i32.const 1) (i32.const 268435456) (i32.const 4))))"#;

    #[test]
    fn out_of_bounds_accesses_return_errors_not_panics() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, OOB_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        // host_log 越界被吞掉;kv_get 越界返回 -1 作为 on_event 的返回值。
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -1);
    }

    /// URL 与 method 校验先于任何网络动作,测试无需异步运行时。
    const HTTP_WAT: &str = r#"
(module
  (import "host" "http_post"
    (func $http_post (param i32 i32 i32 i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "POST")
  (data (i32.const 2048) "http://example.com/hook")
  (data (i32.const 4096) "{}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $http_post (i32.const 1024) (i32.const 4)
                      (i32.const 2048) (i32.const 23)
                      (i32.const 4096) (i32.const 2)
                      (i32.const 8192) (i32.const 256))))"#;

    #[test]
    fn http_post_refuses_plain_http_urls() {
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, HTTP_WAT);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -2);
    }

    #[test]
    fn http_post_refuses_non_post_methods() {
        let wat_text = HTTP_WAT.replace("\"POST\"", "\"GET\"");
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, &wat_text);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert_eq!(on_event.call(&mut h.store, (0, 0)).unwrap(), -3);
    }

    /// 没有异步运行时上下文时,网络失败以 -4 返回而不是 panic(生产里 U4 的
    /// block_in_place 提供上下文;这里同时覆盖"拿不到 runtime"分支)。
    #[test]
    fn http_post_without_a_runtime_returns_an_error_code() {
        // https URL 会走到发请求那一步;当前测试线程没有 tokio runtime。
        let wat_text = HTTP_WAT.replace("http://example.com/hook", "https://example.com/hook");
        let engine = engine();
        let app = app();
        let mut h = spawn(&engine, &app, &wat_text);
        let on_event = h.instance.get_typed_func::<(i32, i32), i32>(&mut h.store, "on_event").unwrap();
        assert!(on_event.call(&mut h.store, (0, 0)).unwrap() < 0);
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
        let err =
            call_on_event(&engine, &app, &plugin, &expiry_event(), 10_000, DEFAULT_TIMEOUT_MS).unwrap_err();
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
        let code = call_on_event(&engine, &app, &plugin, &expiry_event(), 1_000_000_000, 50).unwrap();
        let elapsed = started.elapsed();
        assert_eq!(code, -1, "循环应以 kv_get 的预算耗尽返回码退出");
        assert!(elapsed < Duration::from_secs(5), "应在墙钟预算附近终止,实际 {elapsed:?}");
    }

    /// #16:resp_write_plan 的分支——完整写回、截断到 cap、resp_ptr=0 回落、
    /// 硬上限、无缓冲。纯函数直接驱动,不需要网络。
    #[test]
    fn resp_write_plan_covers_full_truncated_fallback_and_limits() {
        // 完整写回:cap 足够,写全部字节。
        assert_eq!(resp_write_plan(8192, 256, (0, 0), 100), Some((8192, 100)));
        // 截断到 cap:响应比缓冲大,只写 cap 字节。
        assert_eq!(resp_write_plan(8192, 10, (0, 0), 100), Some((8192, 10)));
        // resp_ptr = 0:回落最近一次 host_resp_alloc 的缓冲,截断同样生效。
        assert_eq!(resp_write_plan(0, 0, (4096, 32), 100), Some((4096, 32)));
        // 硬上限:cap 声明成超大值,有效容量仍是 64 KiB(bytes_len 传 MAX
        // 即"只要容量"的用法,下载定界走的就是这条)。
        assert_eq!(resp_write_plan(8192, i32::MAX, (0, 0), usize::MAX), Some((8192, HTTP_RESP_MAX)));
        // 没有可用缓冲:请求已发出的场景由调用方返回 0 字节。
        assert_eq!(resp_write_plan(0, 0, (0, 0), 100), None);
        assert_eq!(resp_write_plan(8192, -1, (0, 0), 100), None);
    }

    // ---- Registry(U4):路由、隔离、预加载、dispatch_log、回写 ----
    //
    // 派发依赖 block_in_place,它只在多线程 runtime 上可用——#[tokio::test]
    // 缺省是 current_thread,必须显式声明 flavor。

    /// on_event 往自己的 kv 命名空间写 called=1:被调没被调,db 里见。
    const KV_CALLED_WAT: &str = r#"
(module
  (import "host" "kv_set" (func $kv_set (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "called")
  (data (i32.const 2048) "1")
  (func (export "__alloc") (param $cap i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (drop (call $kv_set (i32.const 1024) (i32.const 6) (i32.const 2048) (i32.const 1)))
    (i32.const 0)))"#;

    /// 死循环模块:配合小的 fuel 验证 fuel_exhausted,配合大 fuel 与小超时验证
    /// timeout——两个截断路径都需要一个不会自己停的插件。
    const SPIN_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "on_event") (param i32 i32) (result i32)
    (loop (br 0))
    (i32.const 0)))"#;

    /// App 包进 Arc 后初始化 Registry:真实派发路径需要 Weak 回指 upgrade 成功。
    fn runtime_app() -> Arc<App> {
        let app = Arc::new(App::for_test(Db::open(":memory:").unwrap()));
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        app
    }

    /// manifest 文本,subscribes 可变。
    fn manifest_text(plugin_id: &str, subscribes: &[&str]) -> String {
        let list = subscribes.iter().map(|s| format!("\"{s}\"")).collect::<Vec<_>>().join(", ");
        format!(
            "plugin_id = \"{plugin_id}\"\nname = \"{plugin_id}\"\nversion = \"1.0.0\"\nabi_version = 1\nsubscribes = [{list}]"
        )
    }

    /// 只写 db(enabled=1),不碰 Registry:给 init 的预加载路径留一个"db 里有、
    /// 内存里没有"的起点。
    fn insert(app: &App, plugin_id: &str, subscribes: &[&str], wasm: Vec<u8>) -> i64 {
        let row = app
            .db
            .create_plugin(plugin_id, plugin_id, "1.0.0", &manifest_text(plugin_id, subscribes), &wasm, "")
            .unwrap();
        app.db.set_plugin_enabled(row.id, true).unwrap();
        row.id
    }

    /// db 行 + Registry 加载,返回行 id。
    fn install(app: &App, plugin_id: &str, subscribes: &[&str], wasm: Vec<u8>) -> i64 {
        let id = insert(app, plugin_id, subscribes, wasm);
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(app, id).unwrap();
        id
    }

    fn snapshot(app: &App) -> Vec<DispatchEntry> {
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_log_snapshot()
    }

    /// fire-and-forget 的派发是异步完成的;轮询直到谓词满足,10 秒上限。
    async fn until<T>(mut predicate: impl FnMut() -> Option<T>) -> T {
        for _ in 0..400 {
            if let Some(v) = predicate() {
                return v;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("条件在 10 秒内未满足");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribers_of_the_event_are_dispatched_to() {
        let app = runtime_app();
        install(&app, "com.test.a", &["expiry_soon"], compile(KV_CALLED_WAT));
        install(&app, "com.test.b", &["expiry_soon"], compile(KV_CALLED_WAT));
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 2).then_some(s)
        })
        .await;
        for entry in &entries {
            assert_eq!(entry.result, "success", "{entry:?}");
            assert_eq!(entry.event_type, "expiry_soon");
        }
        // 两个插件各写各的命名空间:on_event 真的都跑过了。
        assert_eq!(app.db.get("plugin.com.test.a:called").as_deref(), Some("1"));
        assert_eq!(app.db.get("plugin.com.test.b:called").as_deref(), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_subscribing_to_other_events_is_skipped() {
        let app = runtime_app();
        install(&app, "com.test.a", &["expiry_soon"], compile(KV_CALLED_WAT));
        install(&app, "com.test.b", &["agent_offline"], compile(KV_CALLED_WAT));
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 1).then_some(s)
        })
        .await;
        assert_eq!(entries[0].plugin_id, "com.test.a");
        assert_eq!(app.db.get("plugin.com.test.b:called"), None, "未订阅者不应被调");
    }

    /// KTD10:一个插件加载失败只停用它自己,失败原因落库,其余照常。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_that_fails_to_load_does_not_block_the_rest() {
        let app = Arc::new(App::for_test(Db::open(":memory:").unwrap()));
        let bad = insert(&app, "com.test.bad", &["expiry_soon"], b"\0asm\xde\xad\xbe\xef".to_vec());
        let good = insert(&app, "com.test.good", &["expiry_soon"], compile(KV_CALLED_WAT));
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        {
            let reg = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            assert!(!reg.is_loaded(bad), "坏插件不应进 loaded");
            assert!(reg.is_loaded(good));
        }
        let bad_row = app.db.get_plugin(bad).unwrap().unwrap();
        assert_eq!(bad_row.status, "failed");
        assert!(bad_row.last_error.unwrap().contains("编译失败"), "失败原因要落库");
        // 好插件照常派发。
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        until(|| {
            let s = snapshot(&app);
            (s.len() == 1).then_some(s)
        })
        .await;
        assert_eq!(app.db.get("plugin.com.test.good:called").as_deref(), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_event_with_no_subscribers_records_nothing() {
        let app = runtime_app();
        install(&app, "com.test.a", &["agent_offline"], compile(KV_CALLED_WAT));
        // expiry_soon 没有订阅者:不记 entry,但 bus 的转发计数仍然递增。
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(snapshot(&app).is_empty(), "dispatch_log 只记真的调用了插件的派发");
        assert_eq!(app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_count(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_runaway_plugin_is_cut_off_by_fuel_at_dispatch() {
        let app = runtime_app();
        install(&app, "com.test.spin", &["expiry_soon"], compile(SPIN_WAT));
        app.db.set("plugin.fuel_limit", "10000").unwrap();
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 1).then_some(s)
        })
        .await;
        assert_eq!(entries[0].result, "fuel_exhausted");
        assert_eq!(entries[0].plugin_id, "com.test.spin");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stuck_plugin_is_cut_off_by_the_timeout() {
        let app = runtime_app();
        install(&app, "com.test.spin", &["expiry_soon"], compile(SPIN_WAT));
        // fuel 大到 50ms 烧不完,超时先生效;超时后后台任务继续烧到 fuel 尽头,
        // fuel 是硬兜底,超时只是不再等它。
        app.db.set("plugin.fuel_limit", "400000000").unwrap();
        app.db.set("plugin.timeout_ms", "50").unwrap();
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 1).then_some(s)
        })
        .await;
        assert_eq!(entries[0].result, "timeout");
        assert!(entries[0].elapsed_ms >= 50, "超时前的墙钟至少走满预算:{entries:?}");
    }

    /// 环形淘汰(KTD12)用 dispatch_one 驱动:fire-and-forget 的 dispatch 没有
    /// "全部完成"的等待点,而 dispatch_one 逐次等待,1001 次后确定性断言。
    /// 两条路径写的是同一个 push_entry。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_dispatch_log_is_a_ring_of_one_thousand() {
        let app = runtime_app();
        let id = install(&app, "com.test.a", &["expiry_soon"], compile(MINIMAL_WAT));
        for _ in 0..=DISPATCH_LOG_CAP {
            let entry = app
                .plugins
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .dispatch_one(id, &expiry_event())
                .await
                .unwrap();
            assert_eq!(entry.result, "success");
        }
        assert_eq!(snapshot(&app).len(), DISPATCH_LOG_CAP, "1001 条进 1000 容量的环,最旧一条被淘汰");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fully_successful_dispatch_marks_the_log_row() {
        let app = runtime_app();
        install(&app, "com.test.a", &["expiry_soon"], compile(KV_CALLED_WAT));
        let event = expiry_event();
        let key = event.threshold_or_state_key();
        app.db.record_dispatch(event.node_id(), "expiry_soon", key, 0).unwrap();
        let (ok, detail) = app.db.notification_log_row(7, "expiry_soon", key).unwrap().unwrap();
        assert!(!ok && detail.is_empty(), "派发前 success=0");
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&event);
        let (_, detail) =
            until(|| app.db.notification_log_row(7, "expiry_soon", key).unwrap().filter(|(ok, _)| *ok)).await;
        assert_eq!(detail, "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_partial_failure_keeps_the_row_unsuccessful_and_names_the_failure() {
        let app = runtime_app();
        install(&app, "com.test.good", &["expiry_soon"], compile(KV_CALLED_WAT));
        install(&app, "com.test.bad", &["expiry_soon"], compile(SPIN_WAT));
        // fuel 够 KV 插件跑完一轮、不够死循环停下来:好插件 success,坏插件
        // fuel_exhausted,一好一坏才构成"部分失败"。
        app.db.set("plugin.fuel_limit", "200000").unwrap();
        let event = expiry_event();
        let key = event.threshold_or_state_key();
        app.db.record_dispatch(event.node_id(), "expiry_soon", key, 0).unwrap();
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&event);
        // 两条 entry 都到位后,回写紧随其后;轮询到 detail 非空即回写完成。
        until(|| {
            let s = snapshot(&app);
            (s.len() == 2).then_some(s)
        })
        .await;
        let (ok, detail) = until(|| {
            app.db.notification_log_row(7, "expiry_soon", key).unwrap().filter(|(_, d)| !d.is_empty())
        })
        .await;
        assert!(!ok, "任一插件失败,success 保持 0");
        assert!(detail.contains("com.test.bad: fuel_exhausted"), "点名失败者与原因:{detail}");
        assert!(!detail.contains("com.test.good"), "成功者不进失败名单:{detail}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_one_returns_the_newest_log_entry() {
        let app = runtime_app();
        let id = install(&app, "com.test.a", &["expiry_soon"], compile(MINIMAL_WAT));
        let entry = app
            .plugins
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .dispatch_one(id, &expiry_event())
            .await
            .unwrap();
        assert_eq!(entry.result, "success");
        assert_eq!(entry.event_type, "expiry_soon");
        assert_eq!(entry.plugin_id, "com.test.a");
        let snap = snapshot(&app);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].plugin_id, entry.plugin_id);
        assert_eq!(snap[0].result, entry.result);
        assert_eq!(snap[0].elapsed_ms, entry.elapsed_ms);
        // 未加载的行号:明确的 Err 而不是静默成功。
        let err =
            app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch_one(id + 1, &expiry_event()).await;
        assert!(err.is_err());
    }
}

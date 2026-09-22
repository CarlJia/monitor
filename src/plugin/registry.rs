//! Registry(U4):启动预加载、按 manifest.subscribes 的事件路由、dispatch_log
//! 环形缓冲、notification_log 回写。

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::Serialize;
use tracing::{info, warn};

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::notification_bus::Event;
use crate::App;

use super::host::{
    call_hook, call_json_hook, call_on_event, load, truncate, LoadedPlugin, DEFAULT_FUEL_LIMIT,
    DEFAULT_HOOK_FUEL_LIMIT,
};
use super::log::{new_log_sink, render_log};

/// dispatch_log 的容量(KTD12)。环形:push_back 满了 pop_front,最近 1000 次
/// 派发结果始终可见,面板与测试都据此判断一次派发是否真的发生、结果如何。
pub const DISPATCH_LOG_CAP: usize = 1000;

/// `DispatchEntry::result` 取值 `success` 时表示插件返回 0:Rust 侧的比较与
/// 构造都引用这里,不散写魔法串。
pub const RESULT_SUCCESS: &str = "success";

/// fuel 限额的 setting key(KTD6)。每次派发都重读,改完下一次扫描即生效。
const SETTING_FUEL_LIMIT: &str = "plugin.fuel_limit";
/// ABI v2 数据面钩子(`on_tick`/`render_page`/`on_action`/`on_cleanup`)的 fuel
/// 限额 setting key。与派发分开的理由见 [`DEFAULT_HOOK_FUEL_LIMIT`]。
const SETTING_HOOK_FUEL_LIMIT: &str = "plugin.hook_fuel_limit";
/// 墙钟超时的 setting key(KTD6),单位毫秒。与 fuel 同读,理由相同。
const SETTING_TIMEOUT_MS: &str = "plugin.timeout_ms";
/// 超时缺省值:留出宿主开销后,略宽于插件 http 的 4 秒上限(见
/// [`super::host::HTTP_TIMEOUT`])。
pub(super) const DEFAULT_TIMEOUT_MS: u64 = 5_000;

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
///
/// `result` 是宿主侧的**封闭词表**,`other:2` 这种对操作者毫无信息量——错误码的
/// 含义是插件私有的。所以另有 [`DispatchEntry::detail`]:插件自己经 `host.log`
/// 打的话,失败原因通常就在里面。它只进内存里的派发日志,**不进
/// notification_log**——那张表是宿主的审计,不该混进插件写什么就是什么的自由文本。
#[derive(Debug, Clone, Serialize)]
pub struct DispatchEntry {
    /// Unix 秒。
    pub at: i64,
    /// manifest 的 plugin_id,不是 db 行号:日志是给人看的。
    pub plugin_id: String,
    pub event_type: String,
    pub elapsed_ms: u64,
    pub result: String,
    /// 本次调用里插件自己打的日志(最新几行,有界),没打就是 `None`。
    /// 见 [`super::log::PluginLog`]。
    pub detail: Option<String>,
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

    /// 清空一个插件的派发记录(R16):面板给操作员的逃生门。缓冲是内存里的环形,
    /// 清掉就是丢;长期审计在通知日志表,那里没有「清空」入口。
    ///
    /// 返回实际清掉的条数,面板 toast 据此告诉操作员清掉了多少。0 行也走这条
    /// 路——not 空管道不该把"明明没有记录"包装成失败。push 与本方法共用同
    /// 把互斥锁,清的过程中派发照常写,但都排在锁后;新条目不会同时被丢,
    /// 因为它们进队列的时机就在锁释放之后。
    pub fn clear_dispatch_log(&self, plugin_id: &str) -> usize {
        let mut log = self.dispatch_log.lock().unwrap_or_else(|e| e.into_inner());
        let before = log.len();
        log.retain(|entry| entry.plugin_id != plugin_id);
        before - log.len()
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
                        // 任务本身崩了,run_one 连 entry 都没产出,sink 也留不下来。
                        detail: None,
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
    ///
    /// 与 [`Registry::dispatch_ticks`] 同理:只在取快照时借一次读锁,不横跨
    /// `run_one` 的 await——插件在 on_event 里发事件会重入同一把读锁。
    pub async fn dispatch_one(app: &App, plugin_row_id: i64, event: &Event) -> Result<DispatchEntry> {
        let (engine, dispatch_log, app_arc, plugin) = {
            let reg = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            let Some(plugin) = reg.loaded.get(&plugin_row_id).cloned() else {
                bail!("插件 {plugin_row_id} 未加载");
            };
            let Some(app_arc) = reg.app.upgrade() else {
                bail!("App 已拆除,无法派发");
            };
            (reg.engine.clone(), reg.dispatch_log.clone(), app_arc, plugin)
        };
        let fuel = setting_u64(app, SETTING_FUEL_LIMIT, DEFAULT_FUEL_LIMIT);
        let timeout_ms = setting_u64(app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        let entry = run_one(engine, app_arc, plugin, event.clone(), fuel, timeout_ms).await;
        push_entry(&dispatch_log, entry.clone());
        Ok(entry)
    }

    /// 每小时 tick 派发(U4/KTD4):对所有声明 `tick` 的已启用插件调用 `on_tick`,
    /// 与事件派发同一套 fuel/超时隔离。tick 失败只 warn 不中断其他插件。
    /// 同步执行——housekeeping_pass 是同步的,tick 数量少(每插件一次无参调用),
    /// 不值得再起 fire-and-forget 任务;失败落 dispatch_log 供面板排查。
    ///
    /// 关联函数而非 `&self` 方法:调用方(housekeeping)不再持锁,**这里只在
    /// 取快照的那一小段借一次读锁**。guard 若横跨插件执行,插件在 `on_tick`
    /// 里发事件会经 notification_bus 再取同一把读锁,写者(启停插件)排队时自
    /// 死锁——std 明确把"读后排队写者、再读"列为可能死锁。
    pub fn dispatch_ticks(app: &App) {
        let (engine, dispatch_log, app_arc, plugins) = {
            let reg = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            let Some(app_arc) = reg.app.upgrade() else { return };
            let plugins: Vec<LoadedPlugin> =
                reg.loaded.values().filter(|p| p.manifest.tick).cloned().collect();
            (reg.engine.clone(), reg.dispatch_log.clone(), app_arc, plugins)
        };
        if plugins.is_empty() {
            return;
        }
        let fuel = setting_u64(app, SETTING_HOOK_FUEL_LIMIT, DEFAULT_HOOK_FUEL_LIMIT);
        let timeout_ms = setting_u64(app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        for plugin in plugins {
            run_tick(&engine, &app_arc, &dispatch_log, plugin, fuel, timeout_ms);
        }
    }

    /// 单个插件的 tick：面板启用插件成功后立刻跑一次（见 `api_plugins::enable_plugin`），
    /// 让插件在启用那一刻就对齐一遍宿主状态——插件禁用期间发生的变更不会派发给
    /// 它，只能靠这次和它自己的定时 tick 补上。
    ///
    /// 与 [`Registry::dispatch_ticks`] 同一套 fuel/超时隔离与 dispatch_log 落账，
    /// 同样只在取快照时借一次读锁。插件没声明 `tick` 就什么都不做。
    pub fn dispatch_tick_one(app: &App, plugin_row_id: i64) {
        let (engine, dispatch_log, app_arc, plugin) = {
            let reg = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            let Some(app_arc) = reg.app.upgrade() else { return };
            let Some(plugin) = reg.loaded.get(&plugin_row_id).cloned() else { return };
            (reg.engine.clone(), reg.dispatch_log.clone(), app_arc, plugin)
        };
        if !plugin.manifest.tick {
            return;
        }
        let fuel = setting_u64(app, SETTING_HOOK_FUEL_LIMIT, DEFAULT_HOOK_FUEL_LIMIT);
        let timeout_ms = setting_u64(app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        run_tick(&engine, &app_arc, &dispatch_log, plugin, fuel, timeout_ms);
    }

    /// 调用一个插件的「入参 JSON、返回 JSON」导出(`render_page`/`on_action`/
    /// `on_cleanup`),U5/U9 的页面与清理 API 用。未加载返回 Err(调用方转 404)。
    ///
    /// 与 [`Registry::dispatch_ticks`] 同理:锁只在取快照时借一次,不横跨插件
    /// 执行——插件在 `on_action` 里发事件同样会重入这把读锁。
    pub fn call_json(app: &App, plugin_row_id: i64, hook: &str, input: &[u8]) -> Result<Vec<u8>> {
        let (engine, app_arc, plugin) = {
            let reg = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            let Some(plugin) = reg.loaded.get(&plugin_row_id).cloned() else {
                bail!("插件 {plugin_row_id} 未加载");
            };
            let Some(app_arc) = reg.app.upgrade() else {
                bail!("App 已拆除");
            };
            (reg.engine.clone(), app_arc, plugin)
        };
        let fuel = setting_u64(app, SETTING_HOOK_FUEL_LIMIT, DEFAULT_HOOK_FUEL_LIMIT);
        let timeout_ms = setting_u64(app, SETTING_TIMEOUT_MS, DEFAULT_TIMEOUT_MS);
        call_json_hook(&engine, &app_arc, &plugin, hook, input, fuel, timeout_ms)
    }

    /// 一个已加载插件的 manifest(U5:page 端点要读 page 声明判 404 与标题)。
    pub fn manifest_of(&self, plugin_row_id: i64) -> Option<super::Manifest> {
        self.loaded.get(&plugin_row_id).map(|p| p.manifest.clone())
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
    let event_type = event.type_name().to_owned();
    // sink 建在 spawn **之前**,并由调用方留一份:超时那条分支会放弃后台任务,
    // `call_on_event` 的返回值从此不可达,而那个任务仍在跑、仍握着它的 Arc 克隆。
    // 只有这里这份句柄能看到"被放弃前插件打了什么"。
    let logs = new_log_sink();
    let task_logs = Arc::clone(&logs);
    let task = tokio::spawn(async move {
        tokio::task::block_in_place(move || {
            call_on_event(&engine, &app, &plugin, &event, fuel_limit, timeout_ms, &task_logs)
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
        event_type,
        elapsed_ms: started.elapsed().as_millis() as u64,
        result,
        detail: render_log(&logs, DETAIL_MAX),
    }
}

/// 跑一个插件的 `on_tick` 并把结果落进 dispatch log。每轮一个日志 sink——
/// 跨轮共用会把上一个插件的日志混进这一轮的 detail。
fn run_tick(
    engine: &wasmtime::Engine,
    app: &Arc<App>,
    dispatch_log: &Arc<Mutex<VecDeque<DispatchEntry>>>,
    plugin: LoadedPlugin,
    fuel: u64,
    timeout_ms: u64,
) {
    let plugin_id = plugin.manifest.plugin_id.clone();
    let started = std::time::Instant::now();
    let logs = new_log_sink();
    let result = match call_hook(engine, app, &plugin, "on_tick", fuel, timeout_ms, &logs) {
        Ok(0) => RESULT_SUCCESS.to_owned(),
        Ok(code) => format!("other:{code}"),
        Err(e) => {
            let whole = format!("{e:#}");
            warn!(plugin = %plugin_id, "on_tick 失败: {whole}");
            if whole.to_lowercase().contains("fuel") {
                "fuel_exhausted".to_owned()
            } else {
                format!("host_error:{}", truncate(&whole, DETAIL_MAX))
            }
        }
    };
    push_entry(
        dispatch_log,
        DispatchEntry {
            at: Utc::now().timestamp(),
            plugin_id,
            event_type: "tick".into(),
            elapsed_ms: started.elapsed().as_millis() as u64,
            result,
            detail: render_log(&logs, DETAIL_MAX),
        },
    );
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

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Db;
    use crate::plugin::test_util::{
        compile, expiry_event, insert, install, runtime_app, KV_CALLED_WAT, KV_TICK_WAT, MINIMAL_WAT,
    };

    // ---- Registry(U4):路由、隔离、预加载、dispatch_log、回写 ----
    //
    // 派发依赖 block_in_place,它只在多线程 runtime 上可用——#[tokio::test]
    // 缺省是 current_thread,必须显式声明 flavor。

    /// 死循环模块:配合小的 fuel 验证 fuel_exhausted,配合大 fuel 与小超时验证
    /// timeout——两个截断路径都需要一个不会自己停的插件。
    const SPIN_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "on_event") (param i32 i32) (result i32)
    (loop (br 0))
    (i32.const 0)))"#;

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
        install(&app, "com.test.a", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
        install(&app, "com.test.b", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&expiry_event());
        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 2).then_some(s)
        })
        .await;
        for entry in &entries {
            assert_eq!(entry.result, "success", "{entry:?}");
            assert_eq!(entry.event_type, "plugin_expiry_soon");
        }
        // 两个插件各写各的命名空间:on_event 真的都跑过了。
        assert_eq!(app.db.get("plugin.com.test.a:called").as_deref(), Some("1"));
        assert_eq!(app.db.get("plugin.com.test.b:called").as_deref(), Some("1"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plugin_subscribing_to_other_events_is_skipped() {
        let app = runtime_app();
        install(&app, "com.test.a", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
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
        let bad = insert(&app, "com.test.bad", &["plugin_expiry_soon"], b"\0asm\xde\xad\xbe\xef".to_vec());
        let good = insert(&app, "com.test.good", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
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
        install(&app, "com.test.spin", &["plugin_expiry_soon"], compile(SPIN_WAT));
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

    /// db 行 + Registry 加载,manifest 带 `tick = true`。
    fn install_tick_plugin(app: &App, plugin_id: &str) -> i64 {
        let manifest = format!(
            "plugin_id = \"{plugin_id}\"\nname = \"{plugin_id}\"\nversion = \"1.0.0\"\n\
             abi_version = 2\nsubscribes = []\ntick = true"
        );
        let row = app
            .db
            .create_plugin(plugin_id, plugin_id, "1.0.0", &manifest, &compile(KV_TICK_WAT), "")
            .unwrap();
        app.db.set_plugin_enabled(row.id, true).unwrap();
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(app, row.id).unwrap();
        row.id
    }

    /// 节点生命周期事件按 manifest 的订阅派发,与其它事件同一套过滤。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn node_events_are_dispatched_to_their_subscribers() {
        let app = runtime_app();
        install(&app, "com.test.a", &["node_added", "node_deleted"], compile(KV_CALLED_WAT));
        install(&app, "com.test.b", &["agent_offline"], compile(KV_CALLED_WAT));
        let added = Event::NodeAdded { node_id: 5, name: "edge-1".into(), created_at: 100 };
        let deleted = Event::NodeDeleted { node_id: 5, name: "edge-1".into(), created_at: 100 };
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&added);
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&deleted);

        let entries = until(|| {
            let s = snapshot(&app);
            (s.len() == 2).then_some(s)
        })
        .await;
        for entry in &entries {
            assert_eq!(entry.plugin_id, "com.test.a", "只有订阅了节点事件的插件该收到:{entry:?}");
        }
        // 两个事件各自派发一次,顺序不作断言:派发是 fire-and-forget,两个插件的
        // 任务谁先跑完由调度决定(README 也写明这两个事件可能乱序到达)。
        let types: Vec<&str> = entries.iter().map(|e| e.event_type.as_str()).collect();
        assert!(
            types.contains(&"node_added") && types.contains(&"node_deleted"),
            "两个事件都该派发:{entries:?}"
        );
        assert_eq!(app.db.get("plugin.com.test.a:called").as_deref(), Some("1"), "订阅者真的跑过了");
        assert_eq!(app.db.get("plugin.com.test.b:called"), None, "没订阅的插件不该被调用");
    }

    /// 启用即 tick(dispatch_tick_one)只打指定的那个插件——别的插件的定时
    /// tick 不该被这次调用带着跑。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_tick_one_runs_only_the_named_plugin() {
        let app = runtime_app();
        let a = install_tick_plugin(&app, "com.test.tick.a");
        let _b = install_tick_plugin(&app, "com.test.tick.b");

        Registry::dispatch_tick_one(&app, a);
        let entries = snapshot(&app);
        assert_eq!(entries.len(), 1, "只落一条 tick 记录:{entries:?}");
        assert_eq!(entries[0].plugin_id, "com.test.tick.a");
        assert_eq!(entries[0].event_type, "tick");
        assert_eq!(app.db.get("plugin.com.test.tick.a:called").as_deref(), Some("1"));
        assert_eq!(app.db.get("plugin.com.test.tick.b:called"), None, "B 没被 tick");
    }

    /// 没声明 `tick` 的插件:启用即 tick 对它是空操作,不留日志。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_tick_one_skips_plugins_without_tick() {
        let app = runtime_app();
        let id = install(&app, "com.test.notick", &["agent_offline"], compile(KV_TICK_WAT));
        Registry::dispatch_tick_one(&app, id);
        assert!(snapshot(&app).is_empty(), "没声明 tick 的插件不该被调用");
        assert_eq!(app.db.get("plugin.com.test.notick:called"), None);
    }

    /// ABI v2 的数据面钩子(`on_tick` 与 `render_page`)读的是
    /// `plugin.hook_fuel_limit` 这一档,与派发那档互不影响:派发预算压到 1
    /// 指令,钩子照样跑完;只有压它们自己那一档才截断。两档共用一个 key 的话,
    /// 为钩子调大预算会顺带放宽事件派发的沙箱。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn data_hooks_use_their_own_fuel_budget() {
        const LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (func (export "__alloc") (param i32) (result i32) (i32.const 1024))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0))
  (func $spin (local $i i32)
    (block $done
      (loop $again
        (br_if $done (i32.ge_u (local.get $i) (i32.const 1000)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $again))))
  (func (export "on_tick") (result i32) (call $spin) (i32.const 0))
  (func (export "render_page") (param i32 i32) (result i32) (call $spin) (i32.const 0)))"#;
        let app = runtime_app();
        // tick 要 manifest 声明才会被 dispatch_ticks 挑中;页面钩子走 call_json
        // 直接调,不需要声明。
        let manifest = "plugin_id = \"com.test.hookloop\"\nname = \"hookloop\"\n\
                        version = \"1.0.0\"\nabi_version = 2\nsubscribes = []\ntick = true";
        let row = app
            .db
            .create_plugin("com.test.hookloop", "hookloop", "1.0.0", manifest, &compile(LOOP_WAT), "")
            .unwrap();
        app.db.set_plugin_enabled(row.id, true).unwrap();
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(&app, row.id).unwrap();

        // 1 指令的派发预算:任何事件派发都跑不完。钩子不看这一档。
        app.db.set("plugin.fuel_limit", "1").unwrap();
        assert!(
            Registry::call_json(&app, row.id, "render_page", b"{}").is_ok(),
            "页面钩子不该受 plugin.fuel_limit 约束"
        );
        Registry::dispatch_ticks(&app);
        assert_eq!(snapshot(&app)[0].result, "success", "tick 不该受 plugin.fuel_limit 约束");

        // 压钩子自己那一档 → 两条路径都截断。
        app.db.set("plugin.hook_fuel_limit", "1").unwrap();
        let err = Registry::call_json(&app, row.id, "render_page", b"{}").unwrap_err();
        let whole = format!("{err:#}").to_lowercase();
        assert!(whole.contains("fuel"), "页面钩子应被钩子预算截断,实际: {err:#}");
        Registry::dispatch_ticks(&app);
        // dispatch_log_snapshot 最新在前。
        assert_eq!(snapshot(&app)[0].result, "fuel_exhausted", "tick 应被钩子预算截断");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stuck_plugin_is_cut_off_by_the_timeout() {
        let app = runtime_app();
        install(&app, "com.test.spin", &["plugin_expiry_soon"], compile(SPIN_WAT));
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

    /// 超时路径也要带回插件自己说的话。把 sink 做成 `Arc` 并由调用方预持,首要
    /// 理由就是这条:被放弃的任务其返回值再也拿不到,只有调用方手里那一份读得到
    /// 「放弃前它说了什么」。这条路径原先只被断言过 result=="timeout"。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_timed_out_dispatch_still_carries_what_the_plugin_logged() {
        let msg = "still alive";
        let wat = format!(
            r#"
(module
  (import "host" "log" (func $log (param i32 i32 i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{msg}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $log (i32.const 2) (i32.const 1024) (i32.const {}))
    (loop (br 0))
    (i32.const 0)))"#,
            msg.len()
        );
        let app = runtime_app();
        let id = install(&app, "com.test.stuck-logger", &["plugin_expiry_soon"], compile(&wat));
        // 与 a_stuck_plugin_is_cut_off_by_the_timeout 同一组参数:fuel 大到 50ms
        // 烧不完,超时先生效。
        app.db.set("plugin.fuel_limit", "400000000").unwrap();
        app.db.set("plugin.timeout_ms", "50").unwrap();
        let entry = Registry::dispatch_one(&app, id, &expiry_event()).await.unwrap();
        assert_eq!(entry.result, "timeout");
        assert_eq!(entry.detail.as_deref(), Some(msg), "超时前打的那句话要带出来");
    }

    /// 环形淘汰(KTD12)用 dispatch_one 驱动:fire-and-forget 的 dispatch 没有
    /// "全部完成"的等待点,而 dispatch_one 逐次等待,1001 次后确定性断言。
    /// 两条路径写的是同一个 push_entry。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_dispatch_log_is_a_ring_of_one_thousand() {
        let app = runtime_app();
        let id = install(&app, "com.test.a", &["plugin_expiry_soon"], compile(MINIMAL_WAT));
        for _ in 0..=DISPATCH_LOG_CAP {
            let entry = Registry::dispatch_one(&app, id, &expiry_event()).await.unwrap();
            assert_eq!(entry.result, "success");
        }
        assert_eq!(snapshot(&app).len(), DISPATCH_LOG_CAP, "1001 条进 1000 容量的环,最旧一条被淘汰");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fully_successful_dispatch_marks_the_log_row() {
        let app = runtime_app();
        install(&app, "com.test.a", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
        let event = expiry_event();
        let key = event.threshold_or_state_key();
        app.db.record_dispatch(event.node_id(), "plugin_expiry_soon", key, 0).unwrap();
        let (ok, detail) = app.db.notification_log_row(7, "plugin_expiry_soon", key).unwrap().unwrap();
        assert!(!ok && detail.is_empty(), "派发前 success=0");
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&event);
        let (_, detail) = until(|| {
            app.db.notification_log_row(7, "plugin_expiry_soon", key).unwrap().filter(|(ok, _)| *ok)
        })
        .await;
        assert_eq!(detail, "");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_partial_failure_keeps_the_row_unsuccessful_and_names_the_failure() {
        let app = runtime_app();
        install(&app, "com.test.good", &["plugin_expiry_soon"], compile(KV_CALLED_WAT));
        install(&app, "com.test.bad", &["plugin_expiry_soon"], compile(SPIN_WAT));
        // fuel 够 KV 插件跑完一轮、不够死循环停下来:好插件 success,坏插件
        // fuel_exhausted,一好一坏才构成"部分失败"。
        app.db.set("plugin.fuel_limit", "200000").unwrap();
        let event = expiry_event();
        let key = event.threshold_or_state_key();
        app.db.record_dispatch(event.node_id(), "plugin_expiry_soon", key, 0).unwrap();
        app.plugins.read().unwrap_or_else(|e| e.into_inner()).dispatch(&event);
        // 两条 entry 都到位后,回写紧随其后;轮询到 detail 非空即回写完成。
        until(|| {
            let s = snapshot(&app);
            (s.len() == 2).then_some(s)
        })
        .await;
        let (ok, detail) = until(|| {
            app.db.notification_log_row(7, "plugin_expiry_soon", key).unwrap().filter(|(_, d)| !d.is_empty())
        })
        .await;
        assert!(!ok, "任一插件失败,success 保持 0");
        assert!(detail.contains("com.test.bad: fuel_exhausted"), "点名失败者与原因:{detail}");
        assert!(!detail.contains("com.test.good"), "成功者不进失败名单:{detail}");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_one_returns_the_newest_log_entry() {
        let app = runtime_app();
        let id = install(&app, "com.test.a", &["plugin_expiry_soon"], compile(MINIMAL_WAT));
        let entry = Registry::dispatch_one(&app, id, &expiry_event()).await.unwrap();
        assert_eq!(entry.result, "success");
        assert_eq!(entry.event_type, "plugin_expiry_soon");
        assert_eq!(entry.plugin_id, "com.test.a");
        assert_eq!(entry.detail, None, "MINIMAL_WAT 不打日志,detail 就该是 None");
        let snap = snapshot(&app);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].plugin_id, entry.plugin_id);
        assert_eq!(snap[0].result, entry.result);
        assert_eq!(snap[0].elapsed_ms, entry.elapsed_ms);
        // 未加载的行号:明确的 Err 而不是静默成功。
        let err = Registry::dispatch_one(&app, id + 1, &expiry_event()).await;
        assert!(err.is_err());
    }

    /// 插件返回非 0 时,它自己打的日志要跟着 entry 出来:`other:2` 对操作者毫无
    /// 信息量,原因只可能在这条 detail 里(tg-notify 缺 bot_token 就是这个形状)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_failed_dispatch_carries_the_plugins_own_log_lines() {
        let msg = "kv 里没有 bot_token";
        let wat = format!(
            r#"
(module
  (import "host" "log" (func $log (param i32 i32 i32)))
  (memory (export "memory") 1)
  (data (i32.const 1024) "{msg}")
  (func (export "__alloc") (param i32) (result i32) (i32.const 8192))
  (func (export "on_event") (param i32 i32) (result i32)
    (call $log (i32.const 2) (i32.const 1024) (i32.const {}))
    (i32.const 2)))"#,
            msg.len()
        );
        let app = runtime_app();
        let id = install(&app, "com.test.loggy", &["plugin_expiry_soon"], compile(&wat));
        let entry = Registry::dispatch_one(&app, id, &expiry_event()).await.unwrap();
        assert_eq!(entry.result, "other:2", "宿主侧词表不变,错误码照旧");
        assert_eq!(entry.detail.as_deref(), Some(msg), "插件自己的话要带出来");
        // 只进内存派发日志:notification_log 是宿主的审计表,不混插件自由文本。
        assert_eq!(snapshot(&app)[0].detail.as_deref(), Some(msg), "派发日志同样带着 detail");
    }
}

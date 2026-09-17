#!/usr/bin/env node
// CLS 预算检查：对构建产物 dist/ 逐条后台路由量 Cumulative Layout Shift，
// 超过预算即失败。
//
// 为什么需要它：这类问题的成因几乎都是「某块内容晚于它下面的内容出现，把下面
// 的推了下去」——2026-09-17 安全页就是这样从 0 涨到 0.18 的，而当时的测试只有
// 纯逻辑断言，看不出任何渲染行为。判据不是「有没有抖动」，是「抖动是否超预算」。
//
// 两条硬约束，缺一条这个检查就会骗人：
//   1. 必须有延迟。所有请求瞬间返回时 React 会把渲染合并到一帧，抖动根本不发生，
//      检查恒绿。ENDPOINTS 里逐个端点的 delay 就是为复现真实的错峰到达。
//   2. 必须断言页面真的渲染出了预期内容。请求失败时页面往往只是少一块（CLS 0），
//      只看数字会把「页面根本没渲染」判成「没有抖动」。markers 拦这一手。
import http from "node:http"
import { spawn } from "node:child_process"
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { extname, join, resolve } from "node:path"

const DIST = resolve(import.meta.dirname, "..", "dist")
// web-vitals 的 "good" 阈值是 0.1。预算取它的一半，留出噪声余量，同时远低于
// 安全页回归时的 0.145。
const BUDGET = Number(process.env.CLS_BUDGET ?? 0.05)
const SETTLE_MS = 600
const MARKER_TIMEOUT_MS = 5000

// 一张 1x1 的 PNG，供主题预览图端点使用。图本身不重要，重要的是它在 <img> 的
// onLoad 里才被显示出来——那条路径会撑开卡片。
const PNG = Buffer.from(
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==",
  "base64",
)

const node = (id, name, country) => ({
  id, name, sort: id, public: true, online: true, last_seen: Date.now() / 1000,
  metrics: null, os: "Debian 12", kernel: "6.1.0", arch: "x86_64", virt: "kvm",
  cpu_name: "AMD EPYC", cpu_cores: 4, mem_total: 8, swap_total: 0, disk_total: 100,
  agent_version: "1.0.0", traffic_limit: 0, traffic_mode: "sum", traffic_reset_day: 1,
  total_rx: 0, total_tx: 0, month_rx: 0, month_tx: 0, month_start: "2026-09-01", country,
})

const SETTINGS = {
  site_name: "Monitor",
  github_client_id: "Iv1.0123456789abcdef",
  github_secret_set: true,
  github_allowed_users: "leo",
  retention_days: "7",
  public_page: "on",
  "notification.offline_threshold_reports": "3",
}

// 会话条数决定会话卡片的高度，也就决定回归时把下面两张卡片推多远——三行是
// 复现 0.145 时的取值。
const SESSIONS = [
  { id: "a", current: true, created_at: 1758000000 },
  { id: "b", current: false, created_at: 1757900000 },
  { id: "c", current: false, created_at: 1757880000 },
]

const ENDPOINTS = {
  "/api/me": { delay: 40, body: {} }, // site 在监听后才定得下来，见下方赋值
  "/api/nodes": { delay: 120, body: { admin: true, nodes: [node(1, "hk-1", "HK"), node(2, "jp-1", "JP")] } },
  "/api/settings": { delay: 150, body: SETTINGS },
  "/api/sessions": { delay: 450, body: SESSIONS },
  "/api/ping-tasks": {
    delay: 200,
    body: { tasks: [{ id: 1, name: "东京-延迟", target: "1.1.1.1", interval: 60, nodes: [1] }] },
  },
  "/api/plugins": {
    delay: 250,
    body: [{
      id: 1, plugin_id: "demo", name: "示例插件", version: "1.0.0", enabled: true,
      status: "ok", last_error: null, uploaded_at: 0, subscribes: ["expiry_soon"],
      page: null, tick: false, cleanup: false, config: [],
    }],
  },
  "/api/db": {
    delay: 300,
    body: {
      path: "/var/lib/monitor/monitor.db", size: 1048576, wal: 4096, free: 0,
      oldest: 1757000000, retention: 7, rows: { metric: 1200, ping_record: 340 }, plugins: [],
    },
  },
  "/api/themes": {
    delay: 200,
    body: {
      themes: [{
        name: "默认主题", short: "default", description: "内置", version: "1.0.0",
        author: "monitor", url: "https://github.com/monitor-probe/monitor-theme-default",
        selected: true, builtin: true,
      }],
    },
  },
}

const PREVIEW = /^\/api\/themes\/[^/]+\/preview$/

// markers 全部是「数据到齐才画出来」的文字。用数据本身而不是固定文案，是为了让
// 请求失败（catch 分支渲染空态）也满足不了断言——那正是假通过的高发处。
const ROUTES = [
  { path: "/admin/nodes", markers: ["hk-1", "jp-1"] },
  { path: "/admin/ping", markers: ["东京-延迟"] },
  { path: "/admin/plugins", markers: ["示例插件"] },
  { path: "/admin/data", markers: ["可回收空间", "保留天数"] },
  { path: "/admin/themes", markers: ["默认主题"] },
  { path: "/admin/security", markers: ["登录会话", "GitHub 单点登录", "应急密码"] },
  { path: "/admin/settings", markers: ["站点名称"] },
]

const MIME = {
  ".html": "text/html; charset=utf-8", ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8", ".svg": "image/svg+xml", ".png": "image/png",
  ".json": "application/json", ".ico": "image/x-icon", ".woff2": "font/woff2",
}

// ---- 静态 dist + mock API 同源同进程 ----
// 同源是刻意的：前端用相对路径请求 /api/*，同源就不必往 vite.config 里塞一个
// 只服务于测试的 proxy。
function startServer() {
  const server = http.createServer((req, res) => {
    const { pathname } = new URL(req.url, "http://x")

    if (pathname === "/api/ws") return res.writeHead(400).end()
    if (PREVIEW.test(pathname)) {
      res.writeHead(200, { "content-type": "image/png" })
      return res.end(PNG)
    }
    const endpoint = ENDPOINTS[pathname]
    if (endpoint) {
      return setTimeout(() => {
        res.writeHead(200, { "content-type": "application/json" })
        res.end(JSON.stringify(endpoint.body))
      }, endpoint.delay)
    }
    if (pathname.startsWith("/api/")) return res.writeHead(404).end()

    // /admin/ 下是产物；其余是 SPA 路由，交回 index.html 由前端自己解析路径。
    const rel = pathname.startsWith("/admin/") && !pathname.endsWith("/")
      ? pathname.slice("/admin/".length)
      : ""
    const file = rel && extname(rel) ? join(DIST, rel) : join(DIST, "index.html")
    let body
    try {
      body = readFileSync(file)
    } catch {
      res.writeHead(404).end()
      return
    }
    res.writeHead(200, { "content-type": MIME[extname(file)] ?? "application/octet-stream" })
    res.end(body)
  })

  server.on("upgrade", (_req, socket) => socket.destroy())
  // 端口交给系统挑：写死端口段早晚会和 Chrome 的调试端口撞上，撞上时的表象是
  // 「Chrome 起不来」，排查方向完全跑偏。
  return new Promise((ok, fail) => {
    server.on("error", fail)
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address()
      ENDPOINTS["/api/me"].body = {
        authed: true, github: false, site_name: "Monitor", public_page: true,
        site: `http://127.0.0.1:${port}`, can_provision: false,
      }
      ok({ server, port })
    })
  })
}

// Chrome 的调试端口也要一个空闲的。占用探测与真正监听之间理论上存在竞争，对
// 测试而言无所谓：真的撞上只表现为启动失败，不会得到错误的测量结果。
function freePort() {
  return new Promise((ok, fail) => {
    const probe = http.createServer()
    probe.on("error", fail)
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address()
      probe.close(() => ok(port))
    })
  })
}

// ---- Chrome via CDP ----
// 固定路径之外再扫一遍 PATH：各发行版与 CI 镜像把 Chrome 放哪都可能，猜错路径
// 的代价是 CI 直接失败，多扫一遍就没有这个风险。
function findChrome() {
  const names = ["google-chrome", "google-chrome-stable", "chromium", "chromium-browser", "chrome"]
  const dirs = (process.env.PATH ?? "").split(":").filter(Boolean)
  const candidates = [
    process.env.CHROME_BIN,
    "/usr/bin/google-chrome", "/usr/bin/google-chrome-stable",
    "/opt/google/chrome/google-chrome",
    "/usr/bin/chromium", "/usr/bin/chromium-browser",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ...dirs.flatMap((d) => names.map((n) => join(d, n))),
  ].filter(Boolean)
  const found = candidates.find((p) => existsSync(p))
  if (!found) {
    console.error(`找不到 Chrome。设 CHROME_BIN 指向可执行文件，或安装其一：\n  ${candidates.slice(0, 8).join("\n  ")}`)
    process.exit(2)
  }
  return found
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// 注入到每个文档：把 layout-shift 累加进 window.__cls，并留下每次抖动的归属，
// 失败时能直接指出是哪一块在动。
const OBSERVER = `
window.__cls = 0; window.__shifts = [];
new PerformanceObserver((list) => {
  for (const e of list.getEntries()) {
    if (e.hadRecentInput) continue;
    window.__cls += e.value;
    const label = (n) => {
      if (!n) return "(已移除)";
      const t = (n.textContent || "").replace(/\\s+/g, " ").trim().slice(0, 30);
      return n.nodeName.toLowerCase() + (t ? ' "' + t + '"' : "");
    };
    window.__shifts.push({
      value: Number(e.value.toFixed(4)), at: Math.round(e.startTime),
      sources: (e.sources || []).map((s) => ({
        node: label(s.node),
        dy: Math.round((s.currentRect.y || 0) - (s.previousRect.y || 0)),
      })),
    });
  }
}).observe({ type: "layout-shift", buffered: true });
`

async function measure(chrome, base, profile) {
  const port = await freePort()
  const child = spawn(chrome, [
    "--headless=new", `--remote-debugging-port=${port}`, `--user-data-dir=${profile}`,
    "--no-first-run", "--no-default-browser-check", "--disable-gpu",
    ...(process.env.CI ? ["--no-sandbox", "--disable-dev-shm-usage"] : []),
    "--window-size=1280,900", "about:blank",
  ], { stdio: "ignore" })

  let wsUrl, lastError
  for (let i = 0; i < 100 && !wsUrl; i++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/json/version`)
      wsUrl = (await r.json()).webSocketDebuggerUrl
    } catch (e) {
      lastError = e
    }
    if (!wsUrl) await sleep(150)
  }
  if (!wsUrl) {
    child.kill("SIGKILL")
    throw new Error(`Chrome 未在预期时间内于端口 ${port} 启动（${chrome}）：${lastError}`)
  }

  const ws = new WebSocket(wsUrl)
  await new Promise((ok, fail) => { ws.onopen = ok; ws.onerror = fail })

  let id = 0
  const pending = new Map()
  ws.onmessage = (ev) => {
    const msg = JSON.parse(ev.data)
    if (msg.id && pending.has(msg.id)) { pending.get(msg.id)(msg); pending.delete(msg.id) }
  }
  const send = (method, params = {}, sessionId) => new Promise((ok) => {
    const m = { id: ++id, method, params }
    if (sessionId) m.sessionId = sessionId
    pending.set(m.id, ok)
    ws.send(JSON.stringify(m))
  })

  const { result: { targetId } } = await send("Target.createTarget", { url: "about:blank" })
  const { result: { sessionId } } = await send("Target.attachToTarget", { targetId, flatten: true })
  await send("Page.enable", {}, sessionId)
  await send("Runtime.enable", {}, sessionId)
  await send("Page.addScriptToEvaluateOnNewDocument", { source: OBSERVER }, sessionId)

  const read = async () => {
    const res = await send("Runtime.evaluate", {
      expression: "JSON.stringify({cls: Number((window.__cls ?? 0).toFixed(4)), shifts: window.__shifts ?? [], text: document.body.innerText})",
      returnByValue: true,
    }, sessionId)
    if (res.result.exceptionDetails) throw new Error(JSON.stringify(res.result.exceptionDetails))
    return JSON.parse(res.result.result.value)
  }

  const results = []
  for (const route of ROUTES) {
    await send("Page.navigate", { url: base + route.path }, sessionId)

    // 等 marker 出现（页面确实渲染了）再多等一小段：最后一块数据到达往往正是
    // 抖动发生的时刻，抢在它之前读会漏掉。
    const deadline = Date.now() + MARKER_TIMEOUT_MS
    let snap = await read()
    while (route.markers.some((m) => !snap.text.includes(m)) && Date.now() < deadline) {
      await sleep(100)
      snap = await read()
    }
    const missing = route.markers.filter((m) => !snap.text.includes(m))
    if (missing.length) {
      results.push({ route: route.path, failed: `页面没有渲染出 ${missing.join("、")}（内容断言失败，数字不作数）` })
      continue
    }
    await sleep(SETTLE_MS)
    snap = await read()
    results.push({ route: route.path, cls: snap.cls, shifts: snap.shifts })
  }

  ws.close()
  child.kill("SIGKILL")
  await sleep(200)
  return results
}

// ---- 跑 ----
if (!existsSync(join(DIST, "index.html"))) {
  console.error(`没有构建产物：${DIST}/index.html 不存在。先跑 npm run build。`)
  process.exit(2)
}

const { server, port } = await startServer()
const profile = mkdtempSync(join(tmpdir(), "cls-budget-"))
const cleanup = () => { server.close(); try { rmSync(profile, { recursive: true, force: true }) } catch {} }

let results
try {
  results = await measure(findChrome(), `http://127.0.0.1:${port}`, profile)
} finally {
  cleanup()
}

let failed = false
console.log(`\nCLS 预算 ${BUDGET}（${ROUTES.length} 条路由，1280x900，dist/ 产物）\n`)
for (const r of results) {
  if (r.failed) {
    failed = true
    console.log(`  FAIL  ${r.route}\n        ${r.failed}`)
  } else if (r.cls > BUDGET) {
    failed = true
    console.log(`  FAIL  ${r.route}   CLS ${r.cls} > ${BUDGET}`)
    for (const s of r.shifts) {
      console.log(`        +${s.value} @${s.at}ms ${s.sources.map((x) => `${x.node}(Δy=${x.dy})`).join("  ")}`)
    }
  } else {
    console.log(`  ok    ${r.route}   CLS ${r.cls}`)
  }
}

if (failed) {
  console.log("\n抖动的成因通常是「某块内容晚于它下面的内容出现」。检查该页是否有加载态块排在其它块之上，且未与之同帧渲染。\n")
  process.exit(1)
}
console.log("")
process.exit(0)

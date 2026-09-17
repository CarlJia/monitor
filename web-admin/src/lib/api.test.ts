/// <reference types="node" />
import assert from "node:assert/strict"
import { changes, GIB, provisioningSite, trafficCorrection } from "./api.ts"
import { dispatchResultText, money } from "./format.ts"

assert.deepEqual(changes({ public: true, price: 5 }, { price: 20 }), { price: 20 })
assert.deepEqual(changes({ total_rx: "100", month_tx: "2" }, { total_rx: "100", month_tx: "3" }), { month_tx: "3" })
assert.deepEqual(changes({ expires_at: "2030-01-01" as string | null }, { expires_at: null }), { expires_at: null })
assert.equal(provisioningSite("https://monitor.example.com:8443/"), "https://monitor.example.com:8443")
for (const site of ["http://monitor.example.com", "https://127.0.0.1", "https://[::1]", "https://2130706433", "https://0x7f000001", "https://localhost", "https://user@monitor.example.com", "https://monitor.example.com/path"]) {
  assert.equal(provisioningSite(site), "", site)
}
// An emptied traffic field means the counter is not to be corrected. Sent as 0
// it would clear a lifetime total, which must never decrease.
const shown = { total_rx: "1.5", total_tx: "2", month_rx: "0.25", month_tx: "1" }
assert.deepEqual(trafficCorrection(shown, { ...shown, total_rx: "" }), {})
assert.deepEqual(trafficCorrection(shown, { ...shown, total_rx: "   " }), {})
assert.deepEqual(trafficCorrection(shown, { ...shown, total_rx: "0" }), { total_rx: 0 })
assert.deepEqual(trafficCorrection(shown, { ...shown, total_tx: "3" }), { total_tx: 3 * GIB })
assert.deepEqual(trafficCorrection(shown, shown), {})
assert.equal(money(19.99, "CAD"), "C$19.99")

// 派发结果文案（插件「测试」toast）。没有 detail 时与旧输出逐字相同：这是回归锁。
assert.equal(dispatchResultText({ result: "success", elapsed_ms: 1, detail: null }), "result: success · 耗时 1 ms")
// 有 detail 时插件自己的话领先——`other:2` 光看数字排不了障。
assert.equal(
  dispatchResultText({ result: "other:2", elapsed_ms: 0, detail: "kv 里没有 bot_token" }),
  "kv 里没有 bot_token · result: other:2 · 耗时 0 ms",
)
// 多行 detail 取最后一行：宿主的省略标记在第一行，插件最新打的那条在最后。
assert.equal(
  dispatchResultText({ result: "other:2", elapsed_ms: 0, detail: "…（更早的 4 行已省略）\nline A\nline B" }),
  "line B · result: other:2 · 耗时 0 ms",
)
// 全空白视作没有 detail，不显示一行空白。
assert.equal(
  dispatchResultText({ result: "timeout", elapsed_ms: 5000, detail: "  \n " }),
  "result: timeout · 耗时 5000 ms",
)
// 超长按码点截断：emoji 是代理对，按 UTF-16 下标切会切出半个字符。
assert.equal(
  dispatchResultText({ result: "other:19", elapsed_ms: 3, detail: "🚀".repeat(200) }, 10),
  `${"🚀".repeat(10)}… · result: other:19 · 耗时 3 ms`,
)
// 计数也按码点：120 个 emoji 的 UTF-16 长度是 240，码点数才是 120——刚好到 max
// 就不该截断（单位混用会在这里凭空多出一个省略号）。
assert.equal(
  dispatchResultText({ result: "other:19", elapsed_ms: 1, detail: "🚀".repeat(120) }, 120),
  `${"🚀".repeat(120)} · result: other:19 · 耗时 1 ms`,
)
console.log("partial edits, traffic corrections, provisioning and dispatch-result checks passed")

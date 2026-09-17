/// <reference types="node" />
import assert from "node:assert/strict"
import { changes, formPayload, GIB, httpErrorText, inputType, normalizeFields, provisioningSite, toastKind, trafficCorrection } from "./api.ts"
import type { PluginFieldDecl } from "./api.ts"
import { dispatchResultText, money } from "./format.ts"

// `fields` 来自插件写的 JSON，类型只是断言。下面几例故意喂类型系统不允许的
// 值：归一化必须自己挡住，不能靠调用方守规矩。
const loose = (decls: unknown) => normalizeFields(decls as PluginFieldDecl[])

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
// 非 2xx 的错误文案（api() 与两处上传共用）。空 body、且 HTTP/2/3 下 statusText
// 也为空时，旧写法 `body || statusText` 得到 ""——toast 是一块空白，读起来像
// 「没出错」。必须退回带状态码的一句话，绝不返回空串。
assert.equal(httpErrorText(404, "", ""), "请求失败（HTTP 404）")
assert.equal(httpErrorText(502, "", "  \n "), "请求失败（HTTP 502）")
// 后端给了原因就用它，去掉首尾空白。
assert.equal(httpErrorText(400, "", " 插件包超过 8 MiB 的上限 "), "插件包超过 8 MiB 的上限")
// body 优先于 statusText。
assert.equal(httpErrorText(400, "Bad Request", "缺 bot_token"), "缺 bot_token")
// HTTP/1.1 仍带 statusText：空 body 时用它。
assert.equal(httpErrorText(404, "Not Found", ""), "Not Found")

// 表单字段声明归一化（U1/KTD1）。旧式纯字符串按字段名猜控件，标签就是字段名。
assert.deepEqual(normalizeFields(["name", "price", "unit_cost", "expires_at"]), [
  { name: "name", label: "name", type: "text", options: [] },
  { name: "price", label: "price", type: "number", options: [] },
  { name: "unit_cost", label: "unit_cost", type: "number", options: [] },
  { name: "expires_at", label: "expires_at", type: "date", options: [] },
])
// 没声明 fields（或声明成 null）不是崩溃点：空表头空表单。
assert.deepEqual(normalizeFields(), [])
// 新式声明：标签上列头、类型说了算，`select` 带选项。
assert.deepEqual(
  normalizeFields([
    { name: "name", label: "节点名", type: "text" },
    { name: "fee", label: "费用", type: "number" },
    { name: "currency", label: "币种", type: "select", options: ["CNY", "USD"] },
  ]),
  [
    { name: "name", label: "节点名", type: "text", options: [] },
    // 名字里没有 price/cost/amount，靠声明拿到了数字控件——这正是新声明存在的理由。
    { name: "fee", label: "费用", type: "number", options: [] },
    { name: "currency", label: "币种", type: "select", options: ["CNY", "USD"] },
  ],
)
// label 缺省或只剩空白时用字段名，不渲染一格空表头。
assert.deepEqual(normalizeFields([{ name: "price", type: "number" }]), [
  { name: "price", label: "price", type: "number", options: [] },
])
assert.deepEqual(normalizeFields([{ name: "price", label: "  ", type: "number" }]), [
  { name: "price", label: "price", type: "number", options: [] },
])
// 认不出的 type 回退到字段名启发式（插件写错一个词不该把整列变成文本框）。
assert.deepEqual(normalizeFields([{ name: "price", type: "currency" }]), [
  { name: "price", label: "price", type: "number", options: [] },
])
assert.deepEqual(normalizeFields([{ name: "fee", type: "number " }]), [
  { name: "fee", label: "fee", type: "number", options: [] },
])
// `select` 没给可选项：空下拉是死控件（既不能改也不能清），回退到启发式。
assert.deepEqual(normalizeFields([{ name: "billing_cycle", type: "select" }]), [
  { name: "billing_cycle", label: "billing_cycle", type: "text", options: [] },
])
assert.deepEqual(normalizeFields([{ name: "price", type: "select", options: [] }]), [
  { name: "price", label: "price", type: "number", options: [] },
])
// 选项里的垃圾值丢掉，别让下拉渲染出 undefined 项。
assert.deepEqual(loose([{ name: "c", type: "select", options: ["CNY", 7, null, "USD"] }]), [
  { name: "c", label: "c", type: "select", options: ["CNY", "USD"] },
])
// 类型是断言不是保证：JSON 里什么都可能出现，没有名字的条目只能丢掉。
assert.deepEqual(loose([null, 5, { label: "无名字" }, { name: "" }, { name: "  " }]), [])
assert.deepEqual(loose([{ name: " name ", label: "名称" }]), [
  { name: "name", label: "名称", type: "text", options: [] },
])

// 旧式声明按字段名猜控件的那条启发式：价格类给数字框、`at` 结尾给日期框，
// 其余文本。它只认这几个词，所以插件该显式写 type（见上面的回退用例）。
assert.equal(inputType("price"), "number")
assert.equal(inputType("unit_cost"), "number")
assert.equal(inputType("expires_at"), "date")
assert.equal(inputType("currency"), "text")

const form = normalizeFields([
  { name: "name", label: "节点名", type: "text" },
  { name: "price", label: "价格", type: "number" },
  { name: "currency", label: "币种", type: "select", options: ["CNY", "USD"] },
  { name: "expires_at", label: "到期日", type: "date" },
])
// 提交载荷：id 领队，数字字段强转成 number，其余原样字符串。
assert.deepEqual(
  formPayload(form, 1, { name: "edge-1", price: "12.5", currency: "USD", expires_at: "2027-01-01" }),
  { id: 1, name: "edge-1", price: 12.5, currency: "USD", expires_at: "2027-01-01" },
)
// 没有 id 的行（新增行）不带 id 键，而不是带上 undefined。
assert.deepEqual(formPayload(normalizeFields(["name"]), undefined, { name: "edge-1" }), { name: "edge-1" })
// 数字字段清空表示"不改这个字段"：`Number("")` 是 0，存成 0 会被读成「免费」。
assert.deepEqual(formPayload(form, 1, { name: "edge-1", price: "", currency: "USD" }), {
  id: 1, name: "edge-1", currency: "USD", expires_at: "",
})
assert.deepEqual(formPayload(form, 1, { price: "   " }), {
  id: 1, name: "", currency: "", expires_at: "",
})
// 非空但解析不出数字的同样跳过，保留服务端原值。
assert.deepEqual(formPayload(form, 1, { price: "12,5" }), {
  id: 1, name: "", currency: "", expires_at: "",
})
assert.deepEqual(formPayload(form, 1, { price: "0" }), {
  id: 1, name: "", price: 0, currency: "", expires_at: "",
})
// 数字控件由声明驱动,不看字段名：叫 fee 也一样强转。
assert.deepEqual(formPayload(normalizeFields([{ name: "fee", type: "number" }]), 3, { fee: "3" }), { id: 3, fee: 3 })
assert.deepEqual(formPayload(normalizeFields([{ name: "fee", type: "number" }]), 3, { fee: "3x" }), { id: 3 })

// 提示 kind：四种照原样，缺省或认不出的回退 success（不静默丢提示）。
assert.equal(toastKind("success"), "success")
assert.equal(toastKind("error"), "error")
assert.equal(toastKind("info"), "info")
assert.equal(toastKind("warning"), "warning")
assert.equal(toastKind(undefined), "success")
assert.equal(toastKind("warn"), "success")
assert.equal(toastKind(""), "success")

console.log("partial edits, traffic corrections, provisioning, page-vocabulary and dispatch-result checks passed")

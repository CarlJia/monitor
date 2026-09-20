const UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]

const unitOf = (n: number) => Math.min(Math.floor(Math.log(n) / Math.log(1024)), UNITS.length - 1)

/**
 * 1024-based, as VPS dashboards and `df` report bytes, but labelled MB/GB the way
 * `df -h` and hosting plans write them. Three significant digits by default. Kept
 * in step with the theme's copy of this file.
 */
export function bytes(n: number, digits?: number): string {
  // `< 1` rather than `< 0`: a fraction of a byte puts `unitOf` at -1 and prints
  // "512 undefined".
  if (!n || n < 1) return "0 B"
  const i = unitOf(n)
  const v = n / 1024 ** i
  return `${v.toFixed(i === 0 ? 0 : (digits ?? (v >= 100 ? 0 : v >= 10 ? 1 : 2)))} ${UNITS[i]}`
}

export function uptime(seconds: number): string {
  if (!seconds) return "—"
  const d = Math.floor(seconds / 86400)
  const h = Math.floor((seconds % 86400) / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  return d > 0 ? `${d} 天 ${h} 小时` : h > 0 ? `${h} 小时 ${m} 分` : `${m} 分`
}

/**
 * No expiry and no traffic cap are both rendered as the absence of a ceiling.
 * U+221E rather than the emoji, which arrives as a coloured tile from whatever
 * font the browser provides; this inherits the text colour and size.
 */
export const FOREVER = "∞"

const SYMBOLS: Record<string, string> = { USD: "$", CNY: "¥", EUR: "€", GBP: "£", JPY: "¥", CAD: "C$" }

export function money(amount: number, currency: string): string {
  return `${SYMBOLS[currency] ?? ""}${amount.toFixed(2)}${SYMBOLS[currency] ? "" : ` ${currency}`}`
}

export const CYCLES: Record<string, string> = {
  monthly: "月付",
  quarterly: "季付",
  semiannual: "半年付",
  yearly: "年付",
  biennial: "两年付",
  triennial: "三年付",
  once: "一次性",
}

/**
 * Usage counted as the plan bills it: summing both directions unconditionally
 * would measure a node billed on upload alone against the wrong figure.
 */
export function monthUsage(node: { month_rx: number; month_tx: number; traffic_mode: string }): number {
  switch (node.traffic_mode) {
    case "up":
      return node.month_tx
    case "down":
      return node.month_rx
    case "max":
      return Math.max(node.month_rx, node.month_tx)
    default:
      return node.month_rx + node.month_tx
  }
}

/**
 * 「测试」toast 的描述文案：插件自己打的日志优先（它就是失败原因），宿主侧的
 * 错误码与耗时随后。detail 是多行文本（宿主只留最新几行），取**最后一行**——
 * 失败原因通常就在插件最后打的那条。没有 detail 时输出与从前逐字相同。
 */
export function dispatchResultText(
  entry: { result: string; elapsed_ms: number; detail: string | null },
  max = 120,
): string {
  const line =
    entry.detail
      ?.split("\n")
      .map((l) => l.trim())
      .filter(Boolean)
      .pop() ?? null
  // 两侧都按码点：插件文案常以 emoji 开头，UTF-16 的 length/slice 会把代理对切成
  // 半个字符；而且单位混用会让「码点刚好不超、UTF-16 长度超了」的行凭空多一个省略号。
  const cps = line === null ? [] : [...line]
  const reason = cps.length > max ? `${cps.slice(0, max).join("")}…` : line
  return [reason, `result: ${entry.result}`, `耗时 ${entry.elapsed_ms} ms`].filter(Boolean).join(" · ")
}

/**
 * 「测试」一次点击会派发多条（插件每声明一条订阅就发一条），这里把逐条结果拼成
 * 一段给人看的话：每条一行「事件名 + 单条文案」。单条文案仍走
 * `dispatchResultText`——错误码对操作员没有信息量，插件自己打的那句才有。
 *
 * 整体成败按「每条都 success」算。一条都没跑起来时（没订阅，或订阅的插件事件都
 * 没声明样例载荷）不算成功，也别让它读成「派发失败」——`dispatched` 让面板把这种
 * 情况弹成 warning 而不是 error。
 */
export function testResultsText(
  results: { event: string; result: string; elapsed_ms: number; detail: string | null }[],
): { ok: boolean; dispatched: boolean; text: string } {
  if (results.length === 0) {
    return { ok: false, dispatched: false, text: "这个插件没有订阅任何事件，没有可测试的通知" }
  }
  // `no_sample` 是「该条没派发」的事实标记：测试端点收到这一类条目时没真发出消息。
  // 面板据此判断要不要把整次测试弹成 warning（一条都没真发）。
  const dispatched = results.some((entry) => entry.result !== "no_sample")
  const head = results.every((entry) => entry.result === "no_sample")
    ? "没有任何一条被派发：订阅的插件事件都没有声明样例载荷"
    : ""
  const lines = results.map((entry) => `${entry.event}：${dispatchResultText(entry)}`)
  return { ok: results.every((entry) => entry.result === "success"), dispatched, text: [head, ...lines].filter(Boolean).join("\n") }
}

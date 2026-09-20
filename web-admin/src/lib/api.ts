import { useEffect, useState } from "react"

export type Metrics = {
  uptime: number
  cpu: number
  load: [number, number, number]
  mem_total: number
  mem_used: number
  swap_total: number
  swap_used: number
  disk_total: number
  disk_used: number
  net_rx: number
  net_tx: number
  total_rx: number
  total_tx: number
  month_rx: number
  month_tx: number
  tcp: number
  udp: number
  procs: number
}

export type Node = {
  id: number
  name: string
  sort: number
  public: boolean
  online: boolean
  last_seen: number
  metrics: Metrics | null
  os: string
  kernel: string
  arch: string
  virt: string
  cpu_name: string
  cpu_cores: number
  mem_total: number
  swap_total: number
  disk_total: number
  agent_version: string
  traffic_limit: number
  traffic_mode: string
  traffic_reset_day: number
  total_rx: number
  total_tx: number
  month_rx: number
  month_tx: number
  month_start: string
  /** Panel only. */
  hostname?: string
  /** ISO 3166-1 alpha-2, derived from the node's address. */
  country: string
  ip?: string
  ipv4?: string
  ipv6?: string
  /**
   * Panel only. Where this node's last connection arrived from, as the hub
   * observed it, or empty for a node that has not connected since the hub
   * started keeping it. The panel prefers an address the node reported about
   * itself and falls back to this one; `ip` is not an input to that choice —
   * it holds the address the country lookup keys on, which may be one the node
   * reported about itself.
   */
  observed_ip?: string
  remark?: string
  /** Panel only. Empty for nodes created before the hub retained a copy. */
  token?: string
}

export type PingTask = { id: number; name: string; target: string; interval: number; nodes: number[] }

/** Form snapshots must never overwrite fields the user did not edit. */
export function changes<T extends object>(initial: T, values: Partial<T>): Partial<T> {
  return Object.fromEntries(Object.entries(values).filter(([key, value]) => value !== initial[key as keyof T])) as Partial<T>
}

export const GIB = 1024 ** 3

/**
 * The traffic fields as a `TrafficPatch`: GB entered by hand, bytes on the wire,
 * and only the counters actually given a value.
 *
 * An emptied field means the counter is left unchanged rather than set to zero.
 * The patch is entirely `Option` and `set_traffic` COALESCEs, so omitting the key
 * expresses that; sending 0 would clear a lifetime total, the one figure that may
 * never decrease and that nothing can recompute. Zeroing deliberately remains one
 * keystroke away.
 */
export function trafficCorrection(
  pristine: Record<string, string>,
  typed: Record<string, string>,
): Record<string, number> {
  return Object.fromEntries(
    Object.entries(changes(pristine, typed))
      .filter(([, value]) => String(value).trim() !== "")
      .map(([key, value]) => [key, Math.round(Number(value) * GIB)]),
  )
}

/** Private, carrier-grade NAT, loopback or link-local: unreachable from outside the machine's own network. */
function isLocalV4(ip: string): boolean {
  const [a, b, c] = ip.split(".").map(Number)
  return a === 0 || a === 10 || a === 127 || a >= 224 ||
    (a === 172 && b >= 16 && b < 32) ||
    (a === 192 && (b === 168 || (b === 0 && c === 0))) ||
    (a === 100 && b >= 64 && b < 128) || (a === 169 && b === 254) ||
    (a === 198 && ((b & 0xfe) === 18 || (b === 51 && c === 100))) ||
    (a === 203 && b === 0 && c === 113)
}

/**
 * Not globally routable. Only 2000::/3 is, so the complement is local — and the
 * documentation/reserved ranges inside it are excluded too, mirroring the way
 * `isLocalV4` excludes the v4 test nets instead of treating every dotted
 * quad as reachable.
 */
function isLocalV6(ip: string): boolean {
  const lower = ip.toLowerCase()
  // The documentation and reserved ranges, matching the way `isLocalV4`
  // excludes the v4 test nets rather than calling every dotted quad reachable:
  // 2001:db8::/32, 2001:2::/48, and Teredo's 2001::/32.
  for (const reserved of ["2001:db8", "2001:2", "2001:0"]) {
    if (lower === reserved || lower.startsWith(`${reserved}:`)) return true
  }
  const head = Number.parseInt(lower.split(":")[0] ?? "", 16)
  return !Number.isFinite(head) || (head & 0xe000) !== 0x2000
}

/** One line per address family, each holding reachable addresses before private ones. */
export type AddressLines = { v4: string[]; v6: string[] }

/**
 * The addresses shown for a node, split by family: IPv4 on one line, IPv6 on
 * the next. Within a family the reachable address leads and a private one
 * follows, because pasting either into an ssh command is why they are shown.
 *
 * The reachable address is the one the node reported about itself when it has
 * one — it is not affected by whatever proxy or CDN fronts the hub — and the
 * address the hub observed the node's connection from otherwise. A node behind
 * NAT reports only its private interface, so the observed address is the only
 * evidence of where it can be reached; `ip` is not consulted, being the geo
 * address the country lookup keys on rather than an observation.
 *
 * Only a globally routable observed address is adopted: a private, loopback,
 * link-local or CGNAT one would be the proxy the hub sits behind, printed as if
 * it were the node's own.
 */
export function addresses(node: Pick<Node, "observed_ip" | "ipv4" | "ipv6">): AddressLines {
  const own = { v4: node.ipv4, v6: node.ipv6 }
  const isLocalOf = { v4: isLocalV4, v6: isLocalV6 }
  // Where the kernel lets one socket serve both families, an IPv4 node's peer
  // arrives as `::ffff:a.b.c.d` and is stored in that form. That is an IPv4
  // address wearing a v6 wrapper: unwrap it, or the split below files it under
  // v6 and the IPv4 line loses the only evidence of a reachable address it has.
  const raw = node.observed_ip ?? ""
  const observed = /^::ffff:(\d{1,3}(?:\.\d{1,3}){3})$/i.exec(raw)?.[1] ?? raw
  const observedFamily = observed.includes(":") ? "v6" : observed.includes(".") ? "v4" : ""

  const lines: AddressLines = { v4: [], v6: [] }
  for (const family of ["v4", "v6"] as const) {
    const mine = own[family]
    const isPrivate = Boolean(mine && isLocalOf[family](mine))
    const reachable =
      mine && !isPrivate
        ? mine
        : family === observedFamily && observed && !isLocalOf[family](observed)
          ? observed
          : ""
    if (reachable) lines[family].push(reachable)
    if (mine && isPrivate) lines[family].push(mine)
  }
  return lines
}

/** Installation commands require a TLS origin with a domain, never an IP. */
export function provisioningSite(site: string): string {
  try {
    const u = new URL(site)
    return u.protocol === "https:" && !u.hostname.startsWith("[") && !/^\d+\.\d+\.\d+\.\d+$/.test(u.hostname)
      && u.hostname !== "localhost" && !u.hostname.endsWith(".localhost") && !u.username && !u.password
      && u.pathname === "/" && !u.search && !u.hash ? u.origin : ""
  } catch {
    return ""
  }
}

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

/**
 * 非 2xx 响应给操作者看的一句话。宿主刻意用纯文本体带出中文原因；但空体响应
 * （如 plugin_or_404 的 `StatusCode::NOT_FOUND`）落不到它，而 HTTP/2、HTTP/3
 * 又删掉了 statusText，旧写法 `body || statusText` 于是得到 ""——toast 里是
 * 一块空白，读起来像「没出错」。空到无话可说时退回状态码，绝不返回空串。
 */
export function httpErrorText(status: number, statusText: string, body: string): string {
  return body.trim() || statusText || `请求失败（HTTP ${status}）`
}

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    ...init,
    headers: init?.body ? { "content-type": "application/json", ...init?.headers } : init?.headers,
  })
  if (!res.ok) throw new ApiError(res.status, httpErrorText(res.status, res.statusText, await res.text()))
  return res.status === 204 ? (undefined as T) : res.json()
}

// ---- plugins（U7）：通知插件的上传、启停删、测试、日志与 kv ----

/** manifest 的 `[[config]]` 声明的一项：面板「配置」对话框据此渲染。 */
export type PluginConfigDecl = {
  /** kv 的 key。 */
  key: string
  /** 显示用的人话名字；未声明为 null。 */
  label: string | null
  /** 点「测试」前是否必须有值（宿主在测试前预检）。 */
  required: boolean
  /** 一句话填写说明；未声明为 null。 */
  hint: string | null
  /**
   * 编辑形态：`textarea` 用多行控件，其余（含缺省与认不出的值）用单行输入框。
   * 与插件页 `form` 块那套 `FIELD_TYPES` 是两条渲染路径，互不相干。
   */
  type?: string
  /**
   * 该 key 没有值时对话框里预填的文案——插件内置文案的基准，好让操作员「改一个
   * 字」而不是从空白写起。null 或缺省 = 没有默认值。
   */
  default?: string | null
}

/**
 * 「配置」对话框一行的草稿：当前值、库里的原值、以及 manifest 的声明。
 * `original` 为 null 表示库里还没有这一行。
 */
export type KvDraft = { key: string; value: string; original: string | null; decl?: PluginConfigDecl }

/**
 * 一行在对话框里显示的文案：存量非空白优先，其次声明里的默认值，最后空串。
 *
 * 「空白也算没有」是刻意的：操作员把模板清空保存后，kv 行还在但值是空的，宿主
 * 对空值返回 0、插件回退到内置文案——面板必须与插件看到的一致，所以重新打开
 * 对话框时显示回默认文案，而不是一格看不见的空。
 */
export function kvShownValue(stored: string | null | undefined, decl?: PluginConfigDecl): string {
  const value = stored ?? ""
  if (value.trim() !== "") return value
  return decl?.default ?? ""
}

/**
 * 这一行是否「没被自定义过」——展示值正好是声明里的默认值。
 *
 * 用途是**不落库**：打开对话框什么都没改就点保存，不该把内置文案固化成一条 kv
 * 值（固化了之后插件升级换了内置文案，这份存量值再也跟不上）。代价是一条恰好与
 * 内置文案逐字相同的自定义模板会被当成没自定义——渲染结果一样，只是配置从显式
 * 变回隐式。
 */
export function kvIsDefault(shown: string, decl?: PluginConfigDecl): boolean {
  return decl?.default != null && shown === decl.default
}

/**
 * 一行草稿该写什么：`undefined` = 不动，空串 = 清掉这一行（插件回退到内置
 * 文案），其余是要写的值。调用方负责 key 的取值与形状校验。
 */
export function kvWriteFor(row: KvDraft): string | undefined {
  const asDefault = kvIsDefault(row.value, row.decl)
  if (row.original === null) {
    // 没填完的新行不落库：空值、只有默认值、或 key 还是空的，都不该为它写一行
    // ——落一条空 kv 会让配置列表里多出一行操作员从没创建、也解释不了来源的记录。
    if (row.key.trim() === "" || row.value === "" || asDefault) return undefined
    return row.value
  }
  // 原本自定义过、现在回到默认值：写空把它清掉，而不是留着那份旧文案。
  if (asDefault) return row.original === "" ? undefined : ""
  return row.value === row.original ? undefined : row.value
}

export type Plugin = {
  id: number
  plugin_id: string
  name: string
  version: string
  enabled: boolean
  status: string
  last_error: string | null
  uploaded_at: number
  subscribes: string[]
  /** v2：插件声明的面板页面标题；未声明为 null。 */
  page: string | null
  /** v2：声明了每小时 tick。 */
  tick: boolean
  /** v2：声明了统一的清理入口。 */
  cleanup: boolean
  /**
   * manifest 声明的渠道配置字段；没声明就是空数组。
   * 标成可选是因为它来自 JSON——类型是断言不是保证（`api()` 不做运行时校验），
   * 使用处一律用 `?? []` 兜底。
   */
  config?: PluginConfigDecl[]
}

/** 派发日志的一条（R16）：内存环形缓冲的快照，重启后为空。 */
export type PluginLogEntry = {
  /** Unix 秒。 */
  at: number
  plugin_id: string
  event_type: string
  elapsed_ms: number
  result: string
  /** 插件自己经 host.log 打的话（最新几行，有界）；没打就是 null。 */
  detail: string | null
}

export type PluginKv = { key: string; value: string }

/**
 * 「测试」的一条派发结果。`result` 与派发日志同一套词表（`success` /
 * `other:N` / `timeout` / `host_error:…`），另有一个 `no_sample`——这条订阅是插件
 * 事件而 manifest 没给它声明样例载荷，宿主根本没派发（不是失败，是测不了）。
 */
export type PluginTestResult = { event: string; result: string; elapsed_ms: number; detail: string | null }

/**
 * 上传插件包（R11）。multipart 的 `plugin` 字段带 tar.gz，后端上限 8 MiB。
 * 单独于 `api()`：FormData 不能带 json 的 content-type；413 是代理拦的，
 * 网络断在 fetch 自己身上——两者都要一句人说的话。
 */
export async function uploadPlugin(file: File): Promise<{
  id: number
  plugin_id: string
  /** 这个包里 manifest 声明的版本。 */
  version: string
  status: string
  last_error: string | null
  /**
   * true 表示这次**替换**了同 `plugin_id` 的旧包（上传的版本比已装的高），而
   * 不是首次安装。两种都得由操作员点开关启用，但提示文案不同。
   */
  replaced: boolean
}> {
  const form = new FormData()
  form.append("plugin", file)
  let res: Response
  try {
    res = await fetch("/api/plugins", { method: "POST", body: form })
  } catch {
    throw new ApiError(0, "上传失败，请检查网络")
  }
  if (!res.ok) {
    if (res.status === 413) throw new ApiError(413, "文件过大")
    throw new ApiError(res.status, httpErrorText(res.status, res.statusText, await res.text()))
  }
  return res.json()
}

export const listPlugins = () => api<Plugin[]>("/plugins")

export const deletePlugin = (id: number) => api<void>(`/plugins/${id}`, { method: "DELETE" })

export const enablePlugin = (id: number) =>
  api<{ ok: boolean }>(`/plugins/${id}/enable`, { method: "POST" })

export const disablePlugin = (id: number) =>
  api<{ ok: boolean }>(`/plugins/${id}/disable`, { method: "POST" })

/**
 * 测试通知（R12）：合成一个明天的 ExpirySoon 事件走真实派发路径。
 * 声明了必填 `[[config]]` 而还没填时返回 400，body 是点名缺哪一项的中文说明
 * （`api()` 会把它当 error message 抛出来）。
 */
/** 测试一个插件（R12）：按它声明的订阅逐条真派发，逐条返回结果。 */
export const testPlugin = (id: number) =>
  api<{ plugin_id: string; results: PluginTestResult[] }>(
    `/plugins/${id}/test`,
    { method: "POST" },
  )

/** 一个插件最近的 100 条派发记录（R16）。 */
export const pluginLogs = (id: number) => api<PluginLogEntry[]>(`/plugins/${id}/logs`)

/** 写一个插件的 kv 行（R13）。key 校验在后端 set_plugin_kv。 */
export const setPluginKv = (id: number, key: string, value: string) =>
  api<{ ok: boolean }>(`/plugins/${id}/kv/${encodeURIComponent(key)}`, {
    method: "PUT",
    body: JSON.stringify({ value }),
  })

/** 删一个插件的 kv 行（R13）。204 成功；404 插件不存在；key 校验与 PUT 一致。 */
export const deletePluginKv = (id: number, key: string) =>
  api<void>(`/plugins/${id}/kv/${encodeURIComponent(key)}`, { method: "DELETE" })

/** 列出一个插件的全部 kv 行（R13）。 */
export const listPluginKv = (id: number) => api<PluginKv[]>(`/plugins/${id}/kv`)

/**
 * 下拉选项的两种写法：
 * - 裸字符串：既是提交值也是显示文案；
 * - 对象：`value` 进提交载荷，`label` 只给人看（缺省或全空白用 `value`）。
 *
 * 计费周期这类值是英文标识、文案是中文的字段靠对象形态，币种这类两者相同的
 * 继续写裸字符串。
 */
export type PluginOptionDecl = string | {
  value: string
  label?: string
}

/** 归一化后的选项：渲染看 `label`，提交只认 `value`。 */
export type PluginOption = { value: string; label: string }

/**
 * form 块的一项字段声明（KTD1）。两种形态并存：
 * - 旧式：纯字符串，只给字段名，控件类型交给下面的启发式猜（向后兼容）；
 * - 新式：对象，可带显示用标签、控件类型与下拉选项。
 */
export type PluginFieldDecl = string | {
  /** 提交载荷里的键，也是取值时的键；没有名字的声明会被丢弃。 */
  name: string
  /** 列头文案；缺省（或全空白）用字段名。 */
  label?: string
  /** 控件类型；缺省或认不出时回退到按字段名的启发式。 */
  type?: string
  /** 仅 `type: "select"` 用得上；裸字符串或 `{value, label}` 都接受。 */
  options?: PluginOptionDecl[]
  /**
   * 这一列里显示在值前面的文本取自**同一行**的哪个键（如价格列前的币种符号）；
   * 该键不在字段声明里，所以它只是一格前缀、不会多出一列。缺省或全空白表示
   * 没有前缀。
   */
  prefix_key?: string
}

/**
 * 字段的控件类型名单：既是运行时校验的名单，也是 `FieldType` 的类型来源——
 * 两处各写一遍就会有一处先过期。
 *
 * `money` 是数字的展示形态：右对齐、两位小数（是不是钱由插件声明，面板不按
 * 字段名猜）。提交时按数字处理——格式化后的文本不该漏进载荷。
 */
export const FIELD_TYPES = ["text", "number", "date", "money"] as const

export type FieldType = typeof FIELD_TYPES[number]

/** 归一化后的字段声明：渲染与取值都只看它。 */
export type PluginField = {
  name: string
  /** 列头文案，已兜底成非空。 */
  label: string
  type: FieldType | "select"
  /** 仅 `select` 非空。 */
  options: PluginOption[]
  /** 值前面要显示的文本取自同一行的哪个键；无前缀时是空串。 */
  prefixKey: string
}

/** 取一个可能是任何东西的 JSON 值为字符串；不是字符串就取空串。 */
export function asText(value: unknown): string {
  return typeof value === "string" ? value : ""
}

/**
 * 旧式字段名 → 控件类型：名字里含 `price`/`cost`/`amount`（不分大小写）用数字
 * 输入框，以 `at` 结尾的用日期输入框，其余文本。插件没声明 `type` 时按它猜；
 * 名字不合这套规则就会拿到错误的控件（比如把价格叫 `fee` 只会是文本框），
 * 所以新式声明应当显式写 `type`。
 */
export function inputType(field: string): FieldType {
  if (/price|cost|amount/i.test(field)) return "number"
  if (/at$/.test(field)) return "date"
  return "text"
}

/**
 * 选项声明（裸字符串或 `{value, label}`）→ 渲染用的 `{value, label}`。
 * 页面上的三个下拉（顶层 select 块、form 字段、stat 格子里的控件）共用这一份：
 * 同一件事归一化两遍，迟早有一处漏掉某种形态。
 * `value` 先去空白再判空，不是非空字符串的条目丢掉——下拉里没有值可提交的项
 * 渲染出来就是一格死选项，而只有空白的值渲染出来是一格看不见的选项，两者都
 * 该丢。`label` 缺省或只剩空白时回退到 `value`：显示标识总比显示空白好。
 */
export function normalizeOptions(raw: unknown): PluginOption[] {
  if (!Array.isArray(raw)) return []
  const options: PluginOption[] = []
  for (const entry of raw as unknown[]) {
    // 裸字符串：值即文案。
    const obj = typeof entry === "string" ? { value: entry } : entry
    if (typeof obj !== "object" || obj === null) continue
    const decl = obj as Record<string, unknown>
    const value = asText(decl.value).trim()
    if (value === "") continue
    options.push({ value, label: asText(decl.label).trim() || value })
  }
  return options
}

/**
 * 把 form 块的字段声明归一化成渲染器的输入。声明优先、缺省回退启发式；
 * 声明里认不出的 `type` 同样回退——插件写错一个词不该把整列渲染成废控件。
 *
 * `fields` 来自插件写的 JSON：类型是断言不是保证。这里按垃圾输入防御，
 * 容器不是数组、null、数字、缺 name 的条目一律丢掉，而不是渲染一格空控件
 * 或把页面炸掉（`{}` 与数字连迭代都过不去，字符串则会被逐字符拆成字段）。
 */
export function normalizeFields(decls?: PluginFieldDecl[]): PluginField[] {
  const fields: PluginField[] = []
  if (!Array.isArray(decls)) return []
  for (const raw of decls as unknown[]) {
    const obj = typeof raw === "string" ? { name: raw } : raw
    if (typeof obj !== "object" || obj === null) continue
    const decl = obj as Record<string, unknown>
    const name = asText(decl.name).trim()
    if (name === "") continue
    const options = normalizeOptions(decl.options)
    // 声明值两边可能带空白,先修掉再比对——插件手写 JSON 时很容易多一个空格。
    const declared = asText(decl.type).trim()
    // `select` 没给可选项时也回退：一个没有选项的下拉是死控件（既不能改也
    // 不能清），按字段名猜至少还能操作。
    const type = declared === "select" && options.length > 0 ? "select"
      : isFieldType(declared) ? declared
      : inputType(name)
    fields.push({
      name,
      label: asText(decl.label).trim() || name,
      type,
      options,
      prefixKey: asText(decl.prefix_key).trim(),
    })
  }
  return fields
}

/** `declared` 是否是 `FIELD_TYPES` 里的一员（收窄用，名单只有一份）。 */
function isFieldType(declared: string): declared is FieldType {
  return (FIELD_TYPES as readonly string[]).includes(declared)
}

/**
 * 一行草稿 → 提交载荷（`{id, ...字段}`）。数字字段沿用既有规矩：空串跳过、
 * 解析不出数字的跳过——`Number("")` 是 0，而 0 在价格这类字段里有实际含义
 * （财务插件的 0 表示免费），存成 0 等于静默改掉一个字段；跳过即保留服务端
 * 原值。控件类型看归一化后的声明（`fields`），不再单看字段名。
 *
 * `money` 与 `number` 同路：它只是数字的一个展示形态（两位小数），提交的必须
 * 是数字——把「1200.00」当字符串发回去，插件那侧读不出金额。
 */
export function formPayload(
  fields: PluginField[],
  id: unknown,
  values: Record<string, string>,
): Record<string, unknown> {
  const payload: Record<string, unknown> = {}
  if (id !== undefined) payload.id = id
  for (const f of fields) {
    const raw = values[f.name] ?? ""
    if (f.type !== "number" && f.type !== "money") {
      payload[f.name] = raw
      continue
    }
    const typed = raw.trim()
    if (typed === "") continue
    const n = Number(typed)
    if (!Number.isNaN(n)) payload[f.name] = n
  }
  return payload
}

/**
 * `money` 列的展示值：两位小数。空值返回空串、解析不出数字的值原样返回——
 * 两者都不能被格式化成 `0.00` 或 `NaN`，否则一个没填的价格会读成免费，一个
 * 坏掉的值会从页面上消失。展示成什么样由这一列的声明决定，面板不猜哪列是钱。
 */
export function moneyCell(value: unknown): string {
  const text = value === null || value === undefined ? "" : String(value).trim()
  if (text === "") return ""
  const n = Number(text)
  return Number.isFinite(n) ? n.toFixed(2) : text
}

/**
 * 提示的四种 `kind`：既是运行时校验的名单，也是 `ToastKind` 的类型来源——
 * 两处各写一遍就会有一处先过期。
 */
export const TOAST_KINDS = ["success", "error", "info", "warning"] as const

export type ToastKind = typeof TOAST_KINDS[number]

/** 响应携带的提示条：文案由插件给，面板替它弹一次（KTD2）。 */
export type PluginToast = {
  /**
   * 运行时的值什么都可能是：它来自插件写的 JSON，`api()` 不做校验，所以这里
   * 的类型是文档不是保证。使用处一律经 `toastKind` 收窄（缺省或认不出的回退
   * `success`）。
   */
  kind?: string
  text: string
}

/**
 * 提示的 `kind` → 前端该调哪一个 toast；缺省或认不出的都回退到 `success`
 * （协议只声明了四种，写错的提示宁可当成功也不该静默丢掉）。
 */
export function toastKind(kind?: string): ToastKind {
  return (TOAST_KINDS as readonly string[]).includes(kind ?? "") ? (kind as ToastKind) : "success"
}

/**
 * stat 块里的一格：正常是「标签 + 只读数值」，`select` 存在时这一格是「标签 +
 * 一个下拉控件」（如展示币种的切换）。两者取其一，`value` 与 `select` 同时
 * 出现时按 `select` 渲染。
 */
export type PluginStatItem = {
  label: string
  value?: string
  select?: {
    /** 当前取值；缺省时下拉显示占位。 */
    value?: string
    /** 提交的动作名；缺省时前端按空动作提交。 */
    action?: string
    options?: PluginOptionDecl[]
  }
}

/** 插件面板页面的 JSON UI 描述（U5/KTD5）。前端按词汇表渲染。 */
export type PluginBlock = {
  type: string
  title?: string
  text?: string
  kind?: string
  label?: string
  name?: string
  value?: string
  action?: string
  options?: PluginOptionDecl[]
  items?: PluginStatItem[]
  columns?: string[]
  /**
   * 行的两种形态：table 块是单元格数组，form 块是字段对象。前端用首行形态
   * 区分：数组→表格，对象→表单。
   */
  rows?: unknown[][] | Record<string, unknown>[]
  /** form 块的字段声明；旧式纯字符串或新式带标签/控件/选项的对象。 */
  fields?: PluginFieldDecl[]
}

export type PluginPage = { title?: string; toast?: PluginToast; blocks?: PluginBlock[] }

/** /db 响应的插件空间汇总（U9/KTD11）。宿主只展示，清理由插件自己决定。 */
export type PluginUsage = {
  plugin_id: string
  name: string
  data_rows: number
  data_bytes: number
  kv_bytes: number
}

/** 取一个声明了 page 的插件的页面描述（U5）。 */
export const pluginPage = (id: number) => api<PluginPage>(`/plugins/${id}/page`)

/** 把一次页面交互交给插件处理（U5）。 */
export const pluginAction = (id: number, body: unknown) =>
  api<PluginPage>(`/plugins/${id}/action`, { method: "POST", body: JSON.stringify(body) })

/** 调一个声明了 cleanup 的插件的清理入口（U9/KTD11）。 */
export const pluginCleanup = (id: number) =>
  api<{ freed_bytes: number; pruned: number }>(`/plugins/${id}/cleanup`, { method: "POST" })

/**
 * 4 MiB: the only size a reverse proxy must pass, whatever the file behind it
 * weighs. The hub accepts up to 8 MiB per request, so this can change without
 * touching the server or negotiating first.
 */
const CHUNK = 4 * 1024 * 1024

/**
 * Uploads a file one chunk at a time. There is no upload id: the hub tracks an
 * upload by the length of what it has already written, so a chunk states only
 * where it begins. The last one carries the result.
 */
export async function upload<T>(
  path: string,
  file: File,
  onProgress?: (sent: number) => void,
  signal?: AbortSignal,
): Promise<T> {
  if (file.size === 0) throw new ApiError(400, "文件是空的")
  let last: Response | null = null
  for (let offset = 0; offset < file.size; offset += CHUNK) {
    // A chunk boundary is a genuine stopping point: the hub applies nothing until
    // the last piece lands, and `offset = 0` truncates whatever an abandoned
    // attempt left behind, so aborting here leaves the state unchanged.
    if (signal?.aborted) throw new DOMException("aborted", "AbortError")
    const res = await fetch(`/api${path}?offset=${offset}&total=${file.size}`, {
      method: "POST",
      headers: { "content-type": "application/octet-stream" },
      body: file.slice(offset, offset + CHUNK),
      signal,
    })
    if (!res.ok) {
      // A 413 never reached the hub: the proxy in front answered, and only its
      // own logs record it. The message names the setting responsible.
      throw new ApiError(
        res.status,
        res.status === 413
          ? "反向代理拒收了 4 MiB 的分片，把 nginx 的 client_max_body_size 调到 8m"
          : httpErrorText(res.status, res.statusText, await res.text()),
      )
    }
    last = res
    onProgress?.(Math.min(offset + CHUNK, file.size))
  }
  return last!.json()
}

/**
 * Live node list. Uses the WebSocket the hub pushes every two seconds, falling
 * back to polling if it cannot be established.
 */
export function useNodes() {
  const [nodes, setNodes] = useState<Node[] | null>(null)
  // null until a frame reports it. The panel treats an explicit false as the
  // session no longer being an admin one, so an unanswered first fetch must not
  // read as that; see App.tsx.
  const [admin, setAdmin] = useState<boolean | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [reload, setReload] = useState(0)

  useEffect(() => {
    let socket: WebSocket | null = null
    let poll: ReturnType<typeof setInterval> | null = null
    let retry: ReturnType<typeof setTimeout> | null = null
    let closed = false

    const fetchOnce = () =>
      api<{ nodes: Node[]; admin: boolean }>("/nodes")
        .then((d) => {
          setNodes(d.nodes)
          setAdmin(d.admin)
          setError(null)
        })
        .catch((e: Error) => {
          setError(e.message)
          // With the public page switched off, a revoked session receives a 401
          // here and on the stream, so the frame that would report admin=false
          // never arrives and the panel would retain the list it already had.
          if (e instanceof ApiError && e.status === 401) setAdmin(false)
        })

    fetchOnce()

    const url = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/api/ws`
    // A hub restart closes every stream. Without reconnecting, a page that
    // outlives a deploy would remain on the fallback poll for the rest of its
    // life, refreshing at a fifth of the live rate with no indication.
    const connect = () => {
      try {
        socket = new WebSocket(url)
      } catch {
        poll ??= setInterval(fetchOnce, 5000)
        return
      }
      socket.onmessage = (event) => {
        const frame = JSON.parse(event.data)
        setNodes(frame.nodes)
        setAdmin(frame.admin)
        setError(null)
        // The stream has returned; the poll was only covering for it.
        if (poll) {
          clearInterval(poll)
          poll = null
        }
      }
      socket.onerror = () => socket?.close()
      socket.onclose = () => {
        if (closed) return
        poll ??= setInterval(fetchOnce, 5000)
        retry = setTimeout(connect, 5000)
      }
    }
    connect()

    return () => {
      closed = true
      socket?.close()
      if (poll) clearInterval(poll)
      if (retry) clearTimeout(retry)
    }
  }, [reload])

  return { nodes, admin, error, refresh: () => setReload((n) => n + 1) }
}

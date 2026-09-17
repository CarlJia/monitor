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
  /** ISO 3166-1 alpha-2, derived from the address the agent connects from. */
  country: string
  ip?: string
  ipv4?: string
  ipv6?: string
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
 * 上传插件包（R11）。multipart 的 `plugin` 字段带 tar.gz，后端上限 8 MiB。
 * 单独于 `api()`：FormData 不能带 json 的 content-type；413 是代理拦的，
 * 网络断在 fetch 自己身上——两者都要一句人说的话。
 */
export async function uploadPlugin(file: File): Promise<{
  id: number
  plugin_id: string
  status: string
  last_error: string | null
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
export const testPlugin = (id: number) =>
  api<{ plugin_id: string; wasm_result: string; elapsed_ms: number; detail: string | null }>(
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
  /** 仅 `type: "select"` 用得上。 */
  options?: string[]
}

/** 归一化后的字段声明：渲染与取值都只看它。 */
export type PluginField = {
  name: string
  /** 列头文案，已兜底成非空。 */
  label: string
  type: "text" | "number" | "date" | "select"
  /** 仅 `select` 非空。 */
  options: string[]
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
export function inputType(field: string): "text" | "number" | "date" {
  if (/price|cost|amount/i.test(field)) return "number"
  if (/at$/.test(field)) return "date"
  return "text"
}

/**
 * 把 form 块的字段声明归一化成渲染器的输入。声明优先、缺省回退启发式；
 * 声明里认不出的 `type` 同样回退——插件写错一个词不该把整列渲染成废控件。
 *
 * `fields` 来自插件写的 JSON：类型是断言不是保证。这里按垃圾输入防御，
 * null、数字、缺 name 的条目一律丢掉，而不是渲染一格空控件或把页面炸掉。
 */
export function normalizeFields(decls?: PluginFieldDecl[]): PluginField[] {
  const fields: PluginField[] = []
  for (const raw of (decls ?? []) as unknown[]) {
    const obj = typeof raw === "string" ? { name: raw } : raw
    if (typeof obj !== "object" || obj === null) continue
    const decl = obj as Record<string, unknown>
    const name = asText(decl.name).trim()
    if (name === "") continue
    const options = Array.isArray(decl.options) ? decl.options.filter((o) => typeof o === "string") : []
    // 声明值两边可能带空白,先修掉再比对——插件手写 JSON 时很容易多一个空格。
    const declared = asText(decl.type).trim()
    // `select` 没给可选项时也回退：一个没有选项的下拉是死控件（既不能改也
    // 不能清），按字段名猜至少还能操作。
    const type = declared === "select" && options.length > 0 ? "select"
      : declared === "text" || declared === "number" || declared === "date" ? declared
      : inputType(name)
    fields.push({ name, label: asText(decl.label).trim() || name, type, options })
  }
  return fields
}

/**
 * 一行草稿 → 提交载荷（`{id, ...字段}`）。数字字段沿用既有规矩：空串跳过、
 * 解析不出数字的跳过——`Number("")` 是 0，而 0 在价格这类字段里有实际含义
 * （财务插件的 0 表示免费），存成 0 等于静默改掉一个字段；跳过即保留服务端
 * 原值。控件类型看归一化后的声明（`fields`），不再单看字段名。
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
    if (f.type !== "number") {
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
  options?: string[]
  items?: { label: string; value: string }[]
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

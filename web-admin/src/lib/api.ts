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

export async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`/api${path}`, {
    ...init,
    headers: init?.body ? { "content-type": "application/json", ...init?.headers } : init?.headers,
  })
  if (!res.ok) throw new ApiError(res.status, (await res.text()) || res.statusText)
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
    throw new ApiError(res.status, (await res.text()) || res.statusText)
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
  /** form 块的字段名；编辑后按字段名与 block.action 提交。 */
  fields?: string[]
}

export type PluginPage = { title?: string; blocks?: PluginBlock[] }

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
          : (await res.text()) || res.statusText,
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

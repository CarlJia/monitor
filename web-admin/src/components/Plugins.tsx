import { useEffect, useMemo, useRef, useState } from "react"
import { Eye, EyeOff, KeyRound, LayoutDashboard, Plus, RefreshCw, Trash2, Upload } from "lucide-react"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Textarea } from "@/components/ui/textarea"
import {
  deletePlugin, deletePluginKv, disablePlugin, enablePlugin, kvIsDefault, kvShownValue, kvWriteFor, listPluginKv, listPlugins, pluginCleanup, pluginLogs, setPluginKv, testPlugin, uploadPlugin,
  type KvDraft, type Plugin, type PluginLogEntry,
} from "@/lib/api"
import { testResultsText } from "@/lib/format"

import { ConfirmDialog } from "./ConfirmDialog"

// ---- 插件（U7）：上传、列表、启停删、测试、日志与 kv ----

// 事件类型的徽标配色：与状态页的状态色同义——离线红、上线绿、到期橙。
// key 是 ABI 的事件名，label 是日志筛选复选框上的人话。v2 起宿主自身的到期
// 检测退役，到期提醒由财务插件经 emit_event 发出，事件名带 plugin_ 前缀。
// 这张表还决定派发日志默认显示哪些事件（筛选框与行内徽标都从它派生），所以
// 宿主新增的事件名要一并补在这里，否则它们被默认过滤掉、操作员看不到派发过。
const EVENT_BADGES: Record<string, { label: string; className: string }> = {
  plugin_expiry_soon: { label: "到期提醒", className: "bg-orange-500/15 text-orange-700 dark:text-orange-400" },
  agent_offline: { label: "离线告警", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  agent_online: { label: "上线恢复", className: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400" },
  node_added: { label: "新增节点", className: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400" },
  node_deleted: { label: "删除节点", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  tick: { label: "定时任务", className: "bg-slate-500/15 text-slate-700 dark:text-slate-400" },
}

// 启用绿 / 手动停用灰 / 加载失败红。失败判定先于 enabled：后端回滚 enabled 前，
// 会有短暂 enabled=1 且 status=failed 的窗口，不能让失败插件显示绿色「启用」。
// 失败原因在 last_error 里，徽标太窄放不下，hover 的 title 给出末尾 100 字符
// （错误链的头几层多半是上下文包装）。
function PluginStatus({ plugin }: { plugin: Plugin }) {
  if (plugin.status === "failed" || plugin.last_error) {
    return (
      <Badge variant="destructive" className="font-normal" title={plugin.last_error?.slice(-100) ?? ""}>
        加载失败
      </Badge>
    )
  }
  if (plugin.enabled) return <Badge>启用</Badge>
  return <Badge variant="secondary" className="font-normal">停用</Badge>
}

// kv 行的可编辑状态：original 为 null 的是新行（未保存过，本地移除即可）；
// 已有行点删除时从 rows 摘除并记入 deleted，保存时统一调后端的 DELETE 路由。
// decl 是 manifest 声明过这一项时的展示信息：key 由插件定死，不能改名。
type KvRow = KvDraft

// 渠道配置里常见的凭据字段：默认掩码，眼睛按钮切换可见。
const SECRET_KEY = /token|secret|password/i

function KvDialog({ plugin, onClose }: { plugin: Plugin; onClose: () => void }) {
  const [rows, setRows] = useState<KvRow[] | null>(null)
  const [shown, setShown] = useState<Record<string, boolean>>({})
  const [saving, setSaving] = useState(false)
  // 已有行里被点删除的 key：保存时逐个 DELETE，取消则丢弃（后端不动）。
  const [deleted, setDeleted] = useState<string[]>([])
  // 点击「配置」那一刻的 manifest 声明快照。对话框存续期间它不会变——父组件的
  // plugins 数组重载只换数组，不改这个引用——`useMemo` 把这件事写明，下面的
  // effect 因此只随 plugin.id 重跑。config 来自 JSON，类型是断言而非保证
  // （api() 不做运行时校验），缺了就当这个插件没声明任何字段。
  const decls = useMemo(() => plugin.config ?? [], [plugin.config])

  useEffect(() => {
    listPluginKv(plugin.id)
      .then((pairs) => {
        const stored = new Map(pairs.map(({ key, value }) => [key, value]))
        // 声明过的字段按 manifest 顺序排在最前（带标签、必填标记与提示），没声明
        // 的存量行跟在后面：操作者照声明填，不必猜 key 名，也不会被存量键淹没。
        const declared: KvRow[] = decls.map((decl) => {
          // 存量为空（或根本没有这一行）时展示声明里的默认值：操作员看到的就是
          // 插件内置的那段文案，改一个字即可，而不是从空白写起。
          const stored_ = stored.get(decl.key)
          return { key: decl.key, value: kvShownValue(stored_, decl), original: stored_ ?? null, decl }
        })
        const declaredKeys = new Set(decls.map((d) => d.key))
        const rest: KvRow[] = pairs
          .filter(({ key }) => !declaredKeys.has(key))
          .map(({ key, value }) => ({ key, value, original: value }))
        setRows([...declared, ...rest])
      })
      .catch((e: Error) => { setRows([]); toast.error(e.message) })
  }, [plugin.id, decls])

  const patch = (i: number, next: Partial<KvRow>) =>
    setRows((old) => old?.map((row, j) => (j === i ? { ...row, ...next } : row)) ?? old)

  async function save() {
    if (!rows) return
    // 该写什么、该不该写由 kvWriteFor 决定（纯函数，有测试）——没填完的新行、以及声明了
    // 默认值却没被自定义的字段都不会落库。
    const writes: [string, string][] = []
    for (const row of rows) {
      const value = kvWriteFor(row)
      if (value !== undefined) writes.push([row.original === null ? row.key.trim() : row.key, value])
    }
    for (const [key] of writes) {
      // 长度按**字节**算：服务端比的是 128 字节（plugin::KV_KEY_MAX），而 JS 的
      // key.length 数的是 UTF-16 单元——100 个中文字的 key 会在本地放行、到服务端
      // 才 400。
      if (!key || key.includes(":") || new TextEncoder().encode(key).length > 128) {
        return toast.error(`key「${key}」不合法：非空、不含 ':'、不超过 128 字节`)
      }
    }
    if (!writes.length && !deleted.length) return onClose()
    setSaving(true)
    try {
      // 逐行删/写：后端没有批量接口。一步失败就停并报出是哪一步，已完成的
      // 步骤是真生效了，重开对话框看到的就是当前值。
      for (const key of deleted) await deletePluginKv(plugin.id, key)
      for (const [key, value] of writes) await setPluginKv(plugin.id, key, value)
      toast.success("插件配置已保存")
      onClose()
    } catch (e) {
      toast.error((e as Error).message)
    } finally {
      setSaving(false)
    }
  }

  if (!rows) return null
  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent onOpenAutoFocus={(e) => e.preventDefault()} className="sm:max-w-xl">
        <DialogHeader>
          <DialogTitle>{plugin.name} 的渠道配置</DialogTitle>
          <DialogDescription className="leading-relaxed">
            插件运行时通过 host_kv_get 读这些值（命名空间 <code>plugin.{plugin.plugin_id}:</code>）。
            排在前面的是插件在 plugin.toml 里声明的字段；key 非空、不含 ':'、128 字节内；value 8 KiB 内。
            删除一个已有的 key 会连同值一起从后端移除。
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          {rows.map((row, i) => {
            const decl = row.decl
            // 多行字段用 textarea：掩码那套只对单行输入框有意义，所以多行一律不掩。
            const multiline = decl?.type === "textarea"
            const secret = !multiline && SECRET_KEY.test(row.key)
            const reveal = shown[row.key] ?? false
            const missing = decl?.required && !row.value.trim()
            // 框里显示的就是声明里的默认值 = 还没自定义过：改了才落库，清空即回到它。
            const asDefault = kvIsDefault(row.value, decl)
            return (
              <div key={i} className="space-y-1">
                {decl && (
                  <div className="flex items-center gap-2 text-xs">
                    <span className="font-medium">{decl.label ?? decl.key}</span>
                    {decl.required && <span className="text-destructive">必填</span>}
                    {missing && <span className="text-muted-foreground">还没填，「测试」会被拦下</span>}
                    {asDefault && <span className="text-muted-foreground">未自定义，用插件内置文案</span>}
                  </div>
                )}
                <div className={multiline ? "flex items-start gap-2" : "flex items-center gap-2"}>
                  {/* 已存或声明过的 key 不可改名：改名在后端等于新增一个 key，旧值留在原地。 */}
                  {row.original === null && !decl ? (
                    <Input
                      className="w-44 shrink-0"
                      placeholder="key，如 webhook_url"
                      value={row.key}
                      onChange={(e) => patch(i, { key: e.target.value })}
                    />
                  ) : (
                    <code className="w-44 shrink-0 truncate rounded bg-muted px-2 py-2 text-xs" title={row.key}>
                      {row.key}
                    </code>
                  )}
                  {multiline ? (
                    <Textarea
                      className="min-h-24 min-w-0 flex-1 font-mono text-xs"
                      value={row.value}
                      onChange={(e) => patch(i, { value: e.target.value })}
                    />
                  ) : (
                    <Input
                      type={secret && !reveal ? "password" : "text"}
                      className="min-w-0 flex-1"
                      placeholder="value"
                      value={row.value}
                      autoComplete="off"
                      onChange={(e) => patch(i, { value: e.target.value })}
                    />
                  )}
                  {secret && (
                    <Button
                      variant="ghost"
                      size="icon"
                      title={reveal ? "隐藏" : "显示"}
                      aria-label={reveal ? "隐藏值" : "显示值"}
                      onClick={() => setShown((s) => ({ ...s, [row.key]: !reveal }))}
                    >
                      {reveal ? <EyeOff /> : <Eye />}
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="icon"
                    title={row.original === null ? "移除" : "删除"}
                    aria-label={row.original === null ? "移除" : "删除"}
                    onClick={() =>
                      row.original === null
                        ? setRows((old) => old?.filter((_, j) => j !== i) ?? old)
                        : setRows((old) => {
                            setDeleted((d) => [...d, row.key])
                            return old?.filter((_, j) => j !== i) ?? old
                          })
                    }
                  >
                    <Trash2 className="text-destructive" />
                  </Button>
                </div>
                {decl?.hint && <p className="text-xs text-muted-foreground">{decl.hint}</p>}
              </div>
            )
          })}
          {rows.length === 0 && <p className="text-sm text-muted-foreground">还没有配置项，添加一个 key 开始。</p>}
        </div>
        <div>
          <Button
            variant="outline"
            size="sm"
            onClick={() => setRows((old) => [...(old ?? []), { key: "", value: "", original: null }])}
          >
            <Plus /> 添加
          </Button>
        </div>
        <DialogFooter>
          <Button variant="ghost" onClick={onClose}>取消</Button>
          <Button onClick={save} disabled={saving}>保存</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

// 底部日志卡片（R16）：后端的日志 API 按插件查并分页（缺省单页 50 条），所以
// 这里带一个插件选择器，默认跟第一个插件。事件类型复选筛选仍在本页内做；
// 环形缓冲最多留 1000 条，翻页可以一直翻到最旧。
const LOGS_PAGE_SIZE = 50

function PluginLogsCard({ plugins, pulse }: { plugins: Plugin[]; pulse: number }) {
  const [selected, setSelected] = useState<number | null>(null)
  const [entries, setEntries] = useState<PluginLogEntry[] | null>(null)
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [events, setEvents] = useState<string[]>(Object.keys(EVENT_BADGES))
  const [tick, setTick] = useState(0)
  // 序号化的重拉：翻页、刷新与动作后的并发响应若乱序，只有最新一次落地——
  // 与父组件 loadSeq 同一招，旧页的慢响应不能盖掉新页。
  const seq = useRef(0)
  // 上一轮的「环境」快照：换插件、刷新、动作（pulse）或列表重载都会变。环境
  // 变了页码就拨回第一页——新记录都落在最前，停在旧页只会看错数据。
  const lastEpoch = useRef<string | null>(null)

  // 插件列表变化（上传、删除）时保住仍存在的选择，否则回落到第一个。
  useEffect(() => {
    if (!plugins.length) return setSelected(null)
    setSelected((cur) => (cur != null && plugins.some((p) => p.id === cur) ? cur : plugins[0].id))
  }, [plugins])

  useEffect(() => {
    const epoch = `${selected}-${pulse}-${tick}-${plugins.length}`
    const epochChanged = lastEpoch.current !== epoch
    lastEpoch.current = epoch
    // 先清日志：无论是否跳过，都不该把上一帧（可能属于已删插件）的记录留在屏上。
    setEntries(null)
    // 环境变了而页码停在旧处：这一帧只拨页码、不发请求——页码变化会立刻再触发
    // 本 effect，避免用旧页码多发一次、乱序时把第一页盖掉。
    if (epochChanged && page !== 1) {
      setPage(1)
      return
    }
    // 守卫：selected 不在最新列表里就跳过——别拿已删 id 去请求 logs（404）。删除
    // 后 plugins 更新会让选中项回退，但那次回退所在的渲染里 selected 还是旧值，
    // 这道门就负责拦下那一帧。
    if (selected == null || !plugins.some((p) => p.id === selected)) return
    const s = ++seq.current
    pluginLogs(selected, page, LOGS_PAGE_SIZE)
      .then((r) => {
        if (s !== seq.current) return
        setEntries(r.entries)
        setTotal(r.total)
      })
      .catch((e: Error) => {
        if (s !== seq.current) return
        setEntries([])
        setTotal(0)
        toast.error(e.message)
      })
    // pulse：「测试」（不重拉列表）与「启停」后由父组件递增，触发本 effect 重跑；
    // 上传/删除只重拉列表，靠 plugins 变化触发（见 remove()）。
  }, [selected, pulse, tick, plugins, page])

  if (!plugins.length || selected == null) return null
  const shown = (entries ?? []).filter((entry) => events.includes(entry.event_type))
  const toggle = (type: string) =>
    setEvents((old) => (old.includes(type) ? old.filter((t) => t !== type) : [...old, type]))
  const pages = Math.max(1, Math.ceil(total / LOGS_PAGE_SIZE))

  return (
    <Card className="gap-4 p-5">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="text-sm font-medium">派发日志</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            该插件的派发记录分页展示，新的在前（含「测试」按钮的调用）。缓冲在 hub
            内存里，最多 1000 条，重启后为空；长期审计在通知日志表。
          </p>
        </div>
        <div className="flex items-center gap-2">
          <Select value={String(selected)} onValueChange={(v) => setSelected(Number(v))}>
            <SelectTrigger className="w-52"><SelectValue /></SelectTrigger>
            <SelectContent>
              {plugins.map((p) => (
                <SelectItem key={p.id} value={String(p.id)}>{p.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button variant="outline" size="icon" title="刷新" aria-label="刷新日志" onClick={() => setTick((n) => n + 1)}>
            <RefreshCw />
          </Button>
        </div>
      </div>
      <div className="flex flex-wrap gap-4">
        {Object.entries(EVENT_BADGES).map(([type, meta]) => (
          <label key={type} className="flex cursor-pointer items-center gap-1.5 text-sm">
            <input
              type="checkbox"
              className="accent-primary"
              checked={events.includes(type)}
              onChange={() => toggle(type)}
            />
            {meta.label} <span className="text-xs text-muted-foreground">({type})</span>
          </label>
        ))}
      </div>
      {entries === null ? (
        <p className="text-sm text-muted-foreground">加载中…</p>
      ) : shown.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {entries.length === 0 ? "该插件还没有派发记录" : "没有符合筛选的记录"}
        </p>
      ) : (
        <div className="divide-y rounded-lg border">
          {shown.map((entry, i) => (
            <div key={`${entry.at}-${i}`} className="flex flex-wrap items-center gap-3 px-3 py-2 text-sm">
              <span className="tnum min-w-40 text-muted-foreground">
                {new Date(entry.at * 1000).toLocaleString()}
              </span>
              <Badge className={`font-normal ${EVENT_BADGES[entry.event_type]?.className ?? ""}`} title={entry.event_type}>
                {EVENT_BADGES[entry.event_type]?.label ?? entry.event_type}
              </Badge>
              <span className="tnum text-xs text-muted-foreground">{entry.elapsed_ms} ms</span>
              <span className="flex-1" />
              <Badge
                variant={entry.result === "success" ? "default" : "destructive"}
                className={`font-normal ${entry.result === "success" ? "bg-emerald-600 text-white dark:bg-emerald-600" : ""}`}
                title={entry.result}
              >
                {entry.result}
              </Badge>
              {/* 插件自己经 host.log 打的话：`other:2` 光看数字排不了障。w-full
                  在 flex-wrap 里自成一行；高度由宿主的 500 字节上限兜底。 */}
              {entry.detail && (
                <div
                  className="w-full text-xs break-words whitespace-pre-line text-muted-foreground"
                  title={entry.detail}
                >
                  {entry.detail}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
      {entries !== null && total > 0 && (
        <div className="flex items-center justify-end gap-2 text-xs text-muted-foreground">
          {/* 事件筛选只作用于当前页，页码与条数始终按后端的全部记录算。 */}
          <span className="tnum">
            第 {page} / {pages} 页 · 共 {total} 条
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage((p) => Math.max(1, p - 1))}
          >
            上一页
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= pages}
            onClick={() => setPage((p) => Math.min(pages, p + 1))}
          >
            下一页
          </Button>
        </div>
      )}
    </Card>
  )
}

export function Plugins({ go }: { go: (to: string) => void }) {
  const [plugins, setPlugins] = useState<Plugin[] | null>(null)
  const [uploading, setUploading] = useState(false)
  const [testing, setTesting] = useState<number | null>(null)
  const [cleaning, setCleaning] = useState<number | null>(null)
  const [deleting, setDeleting] = useState<Plugin | null>(null)
  const [removing, setRemoving] = useState(false)
  const [kvFor, setKvFor] = useState<Plugin | null>(null)
  // 动作后递增，让日志卡片跟着刷新。「测试」只能靠它（不重拉列表）；启停额外
  // 重拉列表；删除刻意不 pulse——见 remove()。
  const [pulse, setPulse] = useState(0)
  const picker = useRef<HTMLInputElement>(null)

  // 序号化的重拉：并发的 listPlugins 响应若乱序，只有最新一次的落地——否则删除
  // 前发出的旧请求晚到会把已删行「复活」回列表，日志守卫随之放行。
  const loadSeq = useRef(0)
  const load = () => {
    const seq = ++loadSeq.current
    return listPlugins()
      .then((list) => { if (seq === loadSeq.current) setPlugins(list) })
      .catch((e: Error) => {
        if (seq === loadSeq.current) { setPlugins([]); toast.error(e.message) }
      })
  }
  useEffect(() => { load() }, [])

  const bump = () => setPulse((n) => n + 1)

  async function upload(file: File) {
    setUploading(true)
    try {
      const installed = await uploadPlugin(file)
      // 上传即入库但默认停用（KTD10）：预检失败也一样入库，行上的红徽标会
      // 给出原因，所以这里只引导去点开关，不报错。同名包版本更高时走的是
      // **替换**：行与插件数据都保留，但同样回到停用，要点开关才会装载新包。
      toast.success(
        installed.replaced ? "插件已更新，请点击开关启用新版本" : "插件已上传，请点击开关启用",
        { description: `${installed.plugin_id} · v${installed.version}` },
      )
      load()
    } catch (e) {
      // 400 的响应体就是后端那句中文原因；413 与网络错误在 uploadPlugin 里
      // 已经换成了人说的话。
      toast.error((e as Error).message)
    } finally {
      setUploading(false)
    }
  }

  async function toggle(plugin: Plugin, next: boolean) {
    // 乐观更新：开关先动，失败回滚。无论成败都重拉列表——启用时加载失败会
    // 把行状态写成 failed，红徽标与开关之外的文案得跟着变。
    setPlugins((old) => old?.map((p) => (p.id === plugin.id ? { ...p, enabled: next } : p)) ?? old)
    try {
      await (next ? enablePlugin(plugin.id) : disablePlugin(plugin.id))
    } catch (e) {
      setPlugins((old) => old?.map((p) => (p.id === plugin.id ? { ...p, enabled: !next } : p)) ?? old)
      toast.error((e as Error).message)
    } finally {
      load()
      bump()
    }
  }

  async function test(plugin: Plugin) {
    setTesting(plugin.id)
    try {
      const r = await testPlugin(plugin.id)
      // 一次点击会把每条订阅都真派发一遍（见宿主侧 test_plugin），所以逐条展示，
      // 整体成败按「每条都成功」算；单条文案怎么拼（含插件日志优先）由
      // testResultsText 拥有，这里不重复那套规则。
      const { ok, dispatched, text } = testResultsText(r.results)
      // 一条都没派发（没订阅、或订阅的插件事件都缺样例）不算失败：宿主对这两种情况
      // 返的都是 200,报「派发失败」会让一个完全正常的插件显得坏了。
      const description = <span className="whitespace-pre-line">{text}</span>
      if (!dispatched) {
        toast.warning(`${plugin.name} 没有可测试的通知`, { description })
      } else if (ok) {
        toast.success(`${plugin.name} 测试派发成功`, { description })
      } else {
        toast.error(`${plugin.name} 测试派发失败`, { description })
      }
      bump()
    } catch (e) {
      // 必填配置没填时宿主直接 400，body 就是点名缺哪一项的中文说明。
      toast.error((e as Error).message)
    } finally {
      setTesting(null)
    }
  }

  // 统一清理入口（U9/KTD11）：宿主只转发调用，清理逻辑在插件自己手里。
  async function clean(plugin: Plugin) {
    setCleaning(plugin.id)
    try {
      const r = await pluginCleanup(plugin.id)
      toast.success(`${plugin.name} 清理完成`, {
        description: `回收 ${r.freed_bytes.toLocaleString()} 字节 · 清理 ${r.pruned} 条`,
      })
    } catch (e) {
      toast.error((e as Error).message)
    } finally {
      setCleaning(null)
    }
  }

  async function remove() {
    if (!deleting) return
    setRemoving(true)
    try {
      await deletePlugin(deleting.id)
      toast.success("插件已删除")
      setDeleting(null)
      // 不 pulse：日志卡片由 plugins 更新自己回退选中项并重查。若在这里递增
      // pulse，效果会赶在 load() 落地前、用仍含该 id 的旧列表重跑，带着已删 id
      // 去请求 logs 拿到 404。
      load()
    } catch (e) {
      toast.error((e as Error).message)
    } finally {
      setRemoving(false)
    }
  }

  if (!plugins) return null
  return (
    <div className="space-y-4">
      <Card className="gap-4 p-5">
        <div>
          <h3 className="text-sm font-medium">上传插件</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            插件作者发布的 <code>plugin.tar.gz</code>（plugin.toml + plugin.wasm，上限 8 MiB）。
            上传后默认停用，到列表里点开关启用。插件在 hub 进程内运行，请只安装可信来源。
          </p>
        </div>
        <div>
          <Button size="sm" disabled={uploading} onClick={() => picker.current?.click()}>
            <Upload /> {uploading ? "上传中…" : "上传插件"}
          </Button>
          <input
            ref={picker}
            type="file"
            accept=".gz,.tgz,application/gzip"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0]
              e.target.value = ""
              if (file) upload(file)
            }}
          />
        </div>
      </Card>

      <Card className="overflow-x-auto p-0">
        <Table>
          <TableHeader>
            <TableRow>
              <TableHead className="w-[22%]">名称</TableHead>
              <TableHead className="w-[20%]">插件 ID</TableHead>
              <TableHead className="w-[14%]">状态</TableHead>
              <TableHead className="w-[22%]">订阅事件</TableHead>
              <TableHead className="text-right">操作</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {plugins.map((p) => (
              <TableRow key={p.id}>
                <TableCell>
                  <div className="font-medium">{p.name}</div>
                  <div className="text-xs text-muted-foreground">{p.version}</div>
                </TableCell>
                <TableCell className="text-sm">{p.plugin_id}</TableCell>
                <TableCell>
                  <div className="flex items-center gap-2">
                    <Switch checked={p.enabled} onCheckedChange={(v) => toggle(p, v)} aria-label={`启用 ${p.name}`} />
                    <PluginStatus plugin={p} />
                  </div>
                </TableCell>
                <TableCell>
                  <div className="flex flex-wrap gap-1">
                    {p.subscribes.map((event) => (
                      <Badge key={event} className={`font-normal ${EVENT_BADGES[event]?.className ?? ""}`}>
                        {EVENT_BADGES[event]?.label ?? event}
                      </Badge>
                    ))}
                    {p.subscribes.length === 0 && <span className="text-sm text-muted-foreground">—</span>}
                  </div>
                </TableCell>
                <TableCell className="whitespace-nowrap">
                  <div className="flex items-center justify-end gap-1">
                    {p.page && (
                      <Button variant="ghost" size="sm" onClick={() => go(`/admin/plugins/${p.id}`)} title="打开插件页面">
                        <LayoutDashboard className="size-4" /> 页面
                      </Button>
                    )}
                    <Button variant="ghost" size="sm" disabled={testing === p.id} onClick={() => test(p)}>
                      {testing === p.id ? "测试中…" : "测试"}
                    </Button>
                    <Button variant="ghost" size="sm" onClick={() => setKvFor(p)} title="编辑渠道配置">
                      <KeyRound className="size-4" /> 配置
                    </Button>
                    {p.cleanup && (
                      <Button
                        variant="ghost"
                        size="sm"
                        disabled={cleaning === p.id}
                        onClick={() => clean(p)}
                        title="清理该插件的过期数据"
                      >
                        <RefreshCw className={cleaning === p.id ? "size-4 animate-spin" : "size-4"} />
                        {cleaning === p.id ? "清理中…" : "清理"}
                      </Button>
                    )}
                    <Button variant="ghost" size="icon-sm" onClick={() => setDeleting(p)} title="删除插件" aria-label="删除插件">
                      <Trash2 className="text-destructive" />
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))}
            {plugins.length === 0 && (
              <TableRow>
                <TableCell colSpan={5} className="py-10 text-center text-sm text-muted-foreground">
                  还没有插件，上传一个 plugin.tar.gz 开始
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </Card>

      <PluginLogsCard plugins={plugins} pulse={pulse} />

      {kvFor && <KvDialog plugin={kvFor} onClose={() => setKvFor(null)} />}
      {deleting && (
        <ConfirmDialog
          title={`删除插件「${deleting.name}」？`}
          description="删除后无法恢复，该插件将不再接收任何事件，它的渠道配置（kv）一并删除。"
          confirmLabel="删除插件"
          busy={removing}
          onClose={() => setDeleting(null)}
          onConfirm={remove}
        />
      )}
    </div>
  )
}

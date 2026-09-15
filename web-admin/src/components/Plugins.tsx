import { useEffect, useRef, useState } from "react"
import { Eye, EyeOff, KeyRound, Plus, RefreshCw, Trash2, Upload } from "lucide-react"
import { toast } from "sonner"

import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  deletePlugin, deletePluginKv, disablePlugin, enablePlugin, listPluginKv, listPlugins, pluginLogs, setPluginKv, testPlugin, uploadPlugin,
  type Plugin, type PluginLogEntry,
} from "@/lib/api"

import { ConfirmDialog } from "./ConfirmDialog"

// ---- 插件（U7）：上传、列表、启停删、测试、日志与 kv ----

// 事件类型的徽标配色：与状态页的状态色同义——离线红、上线绿、到期橙。
// key 是 ABI 的事件名，label 是日志筛选复选框上的人话。
const EVENT_BADGES: Record<string, { label: string; className: string }> = {
  expiry_soon: { label: "到期提醒", className: "bg-orange-500/15 text-orange-700 dark:text-orange-400" },
  agent_offline: { label: "离线告警", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  agent_online: { label: "上线恢复", className: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400" },
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
type KvRow = { key: string; value: string; original: string | null }

// 渠道配置里常见的凭据字段：默认掩码，眼睛按钮切换可见。
const SECRET_KEY = /token|secret|password/i

function KvDialog({ plugin, onClose }: { plugin: Plugin; onClose: () => void }) {
  const [rows, setRows] = useState<KvRow[] | null>(null)
  const [shown, setShown] = useState<Record<string, boolean>>({})
  const [saving, setSaving] = useState(false)
  // 已有行里被点删除的 key：保存时逐个 DELETE，取消则丢弃（后端不动）。
  const [deleted, setDeleted] = useState<string[]>([])

  useEffect(() => {
    listPluginKv(plugin.id)
      .then((pairs) => setRows(pairs.map(({ key, value }) => ({ key, value, original: value }))))
      .catch((e: Error) => { setRows([]); toast.error(e.message) })
  }, [plugin.id])

  const patch = (i: number, next: Partial<KvRow>) =>
    setRows((old) => old?.map((row, j) => (j === i ? { ...row, ...next } : row)) ?? old)

  async function save() {
    if (!rows) return
    // 与后端 set_plugin_kv 同一套规则，先在本地过一遍，报错能带上 key。
    const writes: [string, string][] = []
    for (const row of rows) {
      if (row.original === null) {
        // 没填完的新行不保存，而不是挡住整个表单。
        if (row.key.trim()) writes.push([row.key.trim(), row.value])
      } else if (row.value !== row.original) {
        writes.push([row.key, row.value])
      }
    }
    for (const [key] of writes) {
      if (!key || key.includes(":") || key.length > 128) {
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
            key 非空、不含 ':'、128 字节内；value 8 KiB 内。删除一个已有的 key 会连同值一起从后端移除。
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-2">
          {rows.map((row, i) => {
            const secret = SECRET_KEY.test(row.key)
            const reveal = shown[row.key] ?? false
            return (
              <div key={i} className="flex items-center gap-2">
                {/* 已存的 key 不可改名：改名在后端等于新增一个 key，旧值留在原地。 */}
                {row.original === null ? (
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
                <Input
                  type={secret && !reveal ? "password" : "text"}
                  className="min-w-0 flex-1"
                  placeholder="value"
                  value={row.value}
                  autoComplete="off"
                  onChange={(e) => patch(i, { value: e.target.value })}
                />
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

// 底部日志卡片（R16）：后端的日志 API 按插件查，所以这里带一个插件选择器，
// 默认跟第一个插件。事件类型复选筛选，最多显示后端给的最近 100 条。
function PluginLogsCard({ plugins, pulse }: { plugins: Plugin[]; pulse: number }) {
  const [selected, setSelected] = useState<number | null>(null)
  const [entries, setEntries] = useState<PluginLogEntry[] | null>(null)
  const [events, setEvents] = useState<string[]>(Object.keys(EVENT_BADGES))
  const [tick, setTick] = useState(0)

  // 插件列表变化（上传、删除）时保住仍存在的选择，否则回落到第一个。
  useEffect(() => {
    if (!plugins.length) return setSelected(null)
    setSelected((cur) => (cur != null && plugins.some((p) => p.id === cur) ? cur : plugins[0].id))
  }, [plugins])

  useEffect(() => {
    // 删除插件后 pulse 递增会带着旧 id 重查：selected 已不在最新列表里就跳过，
    // 免得和「删除成功」并排弹一个 404。
    if (selected == null || !plugins.some((p) => p.id === selected)) return
    setEntries(null)
    pluginLogs(selected)
      .then(setEntries)
      .catch((e: Error) => { setEntries([]); toast.error(e.message) })
    // pulse：列表页的动作（测试、启停、删除）之后由父组件递增，日志随之刷新。
  }, [selected, pulse, tick, plugins])

  if (!plugins.length || selected == null) return null
  const shown = (entries ?? []).filter((entry) => events.includes(entry.event_type))
  const toggle = (type: string) =>
    setEvents((old) => (old.includes(type) ? old.filter((t) => t !== type) : [...old, type]))

  return (
    <Card className="gap-4 p-5">
      <div className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h3 className="text-sm font-medium">派发日志</h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            该插件最近 100 次派发（含「测试」按钮的调用）。缓冲在 hub 内存里，重启后为空；长期审计在通知日志表。
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
              <Badge className={`font-normal ${EVENT_BADGES[entry.event_type]?.className ?? ""}`}>
                {entry.event_type}
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
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}

export function Plugins() {
  const [plugins, setPlugins] = useState<Plugin[] | null>(null)
  const [file, setFile] = useState<File | null>(null)
  const [uploading, setUploading] = useState(false)
  const [testing, setTesting] = useState<number | null>(null)
  const [deleting, setDeleting] = useState<Plugin | null>(null)
  const [removing, setRemoving] = useState(false)
  const [kvFor, setKvFor] = useState<Plugin | null>(null)
  // 列表上的动作（测试、启停、删除）之后递增，让日志卡片跟着刷新。
  const [pulse, setPulse] = useState(0)
  const fileInput = useRef<HTMLInputElement>(null)

  const load = () =>
    listPlugins().then(setPlugins).catch((e: Error) => { setPlugins([]); toast.error(e.message) })
  useEffect(() => { load() }, [])

  const bump = () => setPulse((n) => n + 1)

  async function upload() {
    if (!file) return
    setUploading(true)
    try {
      const installed = await uploadPlugin(file)
      // 上传即入库但默认停用（KTD10）：预检失败也一样入库，行上的红徽标会
      // 给出原因，所以这里只引导去点开关，不报错。
      toast.success("插件已上传，请点击开关启用", { description: installed.plugin_id })
      setFile(null)
      if (fileInput.current) fileInput.current.value = ""
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
      // result 与耗时分层放：标题说结论，描述留给排查用的细节。
      if (r.wasm_result === "success") {
        toast.success(`${plugin.name} 测试派发成功`, { description: `result: ${r.wasm_result} · 耗时 ${r.elapsed_ms} ms` })
      } else {
        toast.error(`${plugin.name} 测试派发失败`, { description: `result: ${r.wasm_result} · 耗时 ${r.elapsed_ms} ms` })
      }
      bump()
    } catch (e) {
      toast.error((e as Error).message)
    } finally {
      setTesting(null)
    }
  }

  async function remove() {
    if (!deleting) return
    setRemoving(true)
    try {
      await deletePlugin(deleting.id)
      toast.success("插件已删除")
      setDeleting(null)
      load()
      bump()
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
        <div className="flex flex-wrap items-center gap-2">
          <Input
            ref={fileInput}
            type="file"
            accept=".tar.gz,.tgz"
            className="w-auto max-w-sm"
            disabled={uploading}
            onChange={(e) => setFile(e.target.files?.[0] ?? null)}
          />
          <Button size="sm" disabled={!file || uploading} onClick={upload}>
            <Upload /> {uploading ? "上传中…" : "上传插件"}
          </Button>
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
                        {event}
                      </Badge>
                    ))}
                    {p.subscribes.length === 0 && <span className="text-sm text-muted-foreground">—</span>}
                  </div>
                </TableCell>
                <TableCell className="text-right whitespace-nowrap">
                  <Button variant="ghost" size="sm" disabled={testing === p.id} onClick={() => test(p)}>
                    {testing === p.id ? "测试中…" : "测试"}
                  </Button>
                  <Button variant="ghost" size="sm" onClick={() => setKvFor(p)} title="编辑渠道配置">
                    <KeyRound className="size-4" /> 配置
                  </Button>
                  <Button variant="ghost" size="icon" onClick={() => setDeleting(p)} title="删除插件" aria-label="删除插件">
                    <Trash2 className="text-destructive" />
                  </Button>
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

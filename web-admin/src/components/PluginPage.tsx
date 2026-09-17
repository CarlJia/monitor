import { useEffect, useState } from "react"
import { ArrowLeft, Loader2, RefreshCw } from "lucide-react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import {
  asText,
  formPayload,
  normalizeFields,
  pluginAction,
  pluginPage,
  toastKind,
  type PluginBlock,
  type PluginPage as PluginPageData,
  type PluginToast,
  type ToastKind,
} from "@/lib/api"

/** 把任意 JSON 值渲染成可读字符串：null/undefined 视作空，数字/布尔照常。 */
function cell(value: unknown): string {
  return value === null || value === undefined ? "" : String(value)
}

/**
 * `ToastKind` → 该调哪一个 toast。写成 `Record<ToastKind, …>` 是刻意的：
 * 往 `TOAST_KINDS` 里加第五种 kind 时这里会编译不过，而不是被 `switch` 的
 * `default` 静默当成成功。
 */
const TOAST_FN: Record<ToastKind, (m: string) => void> = {
  success: toast.success,
  error: toast.error,
  warning: toast.warning,
  info: toast.info,
}

/**
 * 触发插件随 action 响应带回来的提示。`kind` 缺省或认不出按成功处理
 * （`toastKind` 已收窄，认不出的落到 `success`）；没有文案就不弹（空白
 * toast 读起来像「出错了却没原因」）。
 */
function fireToast(t: PluginToast) {
  const text = asText(t.text)
  if (text.trim() === "") return
  TOAST_FN[toastKind(t.kind)](text)
}

// form 块：每行一组可编辑字段 + 单行保存。草稿按行索引存，页面刷新后
// 由父组件 remount（通过 pageKey 触发）整体清空。
function FormBlock({ block, busy, onSubmit }: {
  block: PluginBlock
  busy: string | null
  onSubmit: (action: string, payload: Record<string, unknown>) => Promise<void>
}) {
  const [drafts, setDrafts] = useState<Record<number, Record<string, string>>>({})
  const rows = (block.rows ?? []) as Record<string, unknown>[]
  const fields = normalizeFields(block.fields)
  // 在途时行内控件一并禁用：响应回来会重挂载表单清空草稿，这几秒里允许编辑
  // 等于允许用户白改一场。
  const busyNow = busy !== null

  const value = (i: number, f: string) => drafts[i]?.[f] ?? cell(rows[i]?.[f])
  const patch = (i: number, f: string, v: string) =>
    setDrafts((old) => ({ ...old, [i]: { ...(old[i] ?? {}), [f]: v } }))

  async function saveRow(row: Record<string, unknown>, i: number) {
    const values = Object.fromEntries(fields.map((f) => [f.name, value(i, f.name)]))
    // 取值规则（数字字段的空串/非数字跳过）在 formPayload 里，见 api.ts。
    await onSubmit(block.action ?? "save", formPayload(fields, row.id, values))
  }

  return (
    <Card className="overflow-x-auto p-0">
      <Table>
        <TableHeader>
          <TableRow>
            {fields.map((f) => <TableHead key={f.name}>{f.label}</TableHead>)}
            <TableHead className="text-right">操作</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.length === 0 ? (
            <TableRow>
              <TableCell colSpan={fields.length + 1} className="py-8 text-center text-sm text-muted-foreground">
                无记录
              </TableCell>
            </TableRow>
          ) : rows.map((row, i) => (
            <TableRow key={i}>
              {fields.map((f) => {
                // 值是「草稿，回落到服务端原值」；placeholder 仍是原值，用户
                // 知道自己抹掉了什么。
                const shown = value(i, f.name)
                const initial = cell(row[f.name])
                // 行里的值可能不在 options 里（插件改过选项集合，老数据还在）：
                // 补一项进去。否则 Radix 的触发器会显示空白，操作者看不出当前
                // 值是什么——正是这次改版要消掉的「看不见」。比对的是选项的
                // `value`（提交载荷里的值），补的那项值与文案都是这串原值：
                // 它没有声明过的文案可用，至少别让当前值消失。
                const extra = shown !== "" && !f.options.some((o) => o.value === shown) ? shown : null
                return (
                  <TableCell key={f.name}>
                    {f.type === "select" ? (
                      <Select
                        value={shown}
                        onValueChange={(v) => patch(i, f.name, v)}
                        disabled={busyNow}
                      >
                        <SelectTrigger className="min-w-32">
                          <SelectValue placeholder={initial} />
                        </SelectTrigger>
                        <SelectContent>
                          {/* 显示 label（人话），提交 value（协议里的标识）。 */}
                          {f.options.map((opt) => (
                            <SelectItem key={opt.value} value={opt.value}>{opt.label}</SelectItem>
                          ))}
                          {extra !== null && <SelectItem key={extra} value={extra}>{extra}</SelectItem>}
                        </SelectContent>
                      </Select>
                    ) : (
                      <Input
                        type={f.type}
                        value={shown}
                        placeholder={initial}
                        className="min-w-32"
                        disabled={busyNow}
                        onChange={(e) => patch(i, f.name, e.target.value)}
                      />
                    )}
                  </TableCell>
                )
              })}
              <TableCell className="text-right whitespace-nowrap">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={busyNow}
                  onClick={() => saveRow(row, i)}
                >
                  保存
                </Button>
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </Card>
  )
}

export function PluginPageView({ id, onBack }: { id: number; onBack: () => void }) {
  const [page, setPage] = useState<PluginPageData | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [reload, setReload] = useState(0)
  const [busy, setBusy] = useState<string | null>(null)
  // 每次拿到新页面对象递增，让 FormBlock 整体重置草稿。
  const [pageKey, setPageKey] = useState(0)

  useEffect(() => {
    let cancelled = false
    pluginPage(id)
      .then((p) => {
        if (cancelled) return
        setPage(p)
        setError(null)
        setPageKey((n) => n + 1)
      })
      .catch((e: Error) => { if (!cancelled) { setPage(null); setError(e.message) } })
    return () => { cancelled = true }
  }, [id, reload])

  // 所有 action 都走同一条路：把 action 与参数发给插件，把返回的页面描述
  // 替换当前页面。busy 锁防止重复点；action 名进入表单的「保存」disabled。
  async function act(action: string, payload: Record<string, unknown>) {
    setBusy(action)
    try {
      const next = await pluginAction(id, { action, ...payload })
      setPage(next)
      setPageKey((n) => n + 1)
      // 提示只在 action 的响应上弹一次；初始 pluginPage 加载不弹——否则每次
      // 打开页面都会重播上一次操作的结果。文案与 kind 都由插件给（KTD2）。
      if (next.toast) fireToast(next.toast)
    } catch (e) {
      toast.error((e as Error).message)
    } finally {
      setBusy(null)
    }
  }

  if (error) {
    return (
      <Card className="gap-4 p-5">
        <p className="text-sm text-destructive">页面加载失败：{error}</p>
        <div>
          <Button size="sm" onClick={() => { setError(null); setReload((n) => n + 1) }}>
            <RefreshCw /> 重试
          </Button>
          <Button size="sm" variant="ghost" className="ml-2" onClick={onBack}>返回插件列表</Button>
        </div>
      </Card>
    )
  }
  // 首次加载可能几秒：插件可以在 `render_page` 里顺手做一次性工作（财务插件
  // 就是在这里拉汇率的），这段时间里只给一句「加载中…」会被读成卡死——多给
  // 一句为什么慢，操作者才知道该等而不是去点刷新。
  if (!page) {
    return (
      <div className="flex flex-col items-center justify-center gap-2 py-12 text-center">
        <Loader2 className="size-5 animate-spin text-muted-foreground" aria-hidden />
        <p className="text-sm text-muted-foreground">加载中…</p>
        <p className="text-xs text-muted-foreground">首次打开可能需要几秒获取汇率</p>
      </div>
    )
  }

  const isTable = (rows: unknown) => Array.isArray(rows) && (rows.length === 0 || Array.isArray(rows[0]))
  const blocks = page.blocks ?? []

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon" onClick={onBack} title="返回" aria-label="返回">
            <ArrowLeft />
          </Button>
          <h2 className="text-base font-medium">{page.title ?? "插件页面"}</h2>
        </div>
        <Button variant="outline" size="icon" onClick={() => setReload((n) => n + 1)} title="刷新" aria-label="刷新页面">
          <RefreshCw />
        </Button>
      </div>

      {blocks.map((block, i) => {
        switch (block.type) {
          case "notice":
            return (
              <div
                key={i}
                className={
                  block.kind === "warning"
                    ? "rounded-lg border border-amber-500/40 bg-amber-500/10 px-3 py-2.5 text-sm text-amber-700 dark:text-amber-400"
                    : "rounded-lg border bg-muted/40 px-3 py-2.5 text-sm"
                }
              >
                {block.text}
              </div>
            )
          case "stat":
            return (
              <Card key={i} className="p-5">
                <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
                  {(block.items ?? []).map((item, j) => (
                    <div key={j}>
                      <div className="text-xs text-muted-foreground">{item.label}</div>
                      <div className="tnum mt-0.5 text-base">{item.value}</div>
                    </div>
                  ))}
                </div>
              </Card>
            )
          case "select": {
            const action = block.action ?? ""
            return (
              <Card key={i} className="p-5">
                <div className="flex flex-wrap items-center gap-3">
                  {block.label && <Label className="shrink-0 text-sm font-medium">{block.label}</Label>}
                  <Select
                    value={block.value ?? ""}
                    onValueChange={(v) => act(action, { value: v })}
                    disabled={busy !== null}
                  >
                    <SelectTrigger className="w-40"><SelectValue /></SelectTrigger>
                    <SelectContent>
                      {(block.options ?? []).map((opt) => (
                        <SelectItem key={opt} value={opt}>{opt}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                </div>
              </Card>
            )
          }
          case "table": {
            const rows = isTable(block.rows) ? (block.rows as unknown[][]) : []
            const columns = block.columns ?? []
            return (
              <Card key={i} className="gap-4 p-5">
                {block.title && <h3 className="text-sm font-medium">{block.title}</h3>}
                <Card className="overflow-x-auto p-0">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        {columns.map((c) => <TableHead key={c}>{c}</TableHead>)}
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {rows.length === 0 ? (
                        <TableRow>
                          <TableCell colSpan={columns.length} className="py-8 text-center text-sm text-muted-foreground">
                            无记录
                          </TableCell>
                        </TableRow>
                      ) : rows.map((row, ri) => (
                        <TableRow key={ri}>
                          {row.map((c, ci) => (
                            <TableCell key={ci} className="text-sm">{String(c ?? "")}</TableCell>
                          ))}
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </Card>
              </Card>
            )
          }
          case "form":
            return (
              <div key={i} className="space-y-2">
                {block.title && <h3 className="text-sm font-medium">{block.title}</h3>}
                {/* key=pageKey：新页面对象到达时重挂载，草稿自然清空。 */}
                <FormBlock key={pageKey} block={block} busy={busy} onSubmit={act} />
              </div>
            )
          default:
            // 未知块类型：忽略但不报错。插件后续可能新增类型，前端跟版不
            // 该把整页挡住。
            return null
        }
      })}
    </div>
  )
}
import { useEffect, useState } from "react"
import { ArrowLeft, RefreshCw } from "lucide-react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Card } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { pluginAction, pluginPage, type PluginBlock, type PluginPage as PluginPageData } from "@/lib/api"

// 字段名 → 输入控件类型。插件不声明每个字段的控件类型，前端从字段名猜：
// 价格/成本用数字，*_at 用日期，其余文本。插件对未声明的字段做兜底处理。
function inputType(field: string): "text" | "number" | "date" {
  if (/price|cost|amount/i.test(field)) return "number"
  if (/at$/.test(field)) return "date"
  return "text"
}

// 把任意 JSON 值渲染成可读字符串：null/undefined 视作空，数字/布尔照常。
function cell(value: unknown): string {
  return value === null || value === undefined ? "" : String(value)
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
  const fields = block.fields ?? []

  const draft = (i: number, f: string) => drafts[i]?.[f] ?? cell(rows[i]?.[f])
  const patch = (i: number, f: string, v: string) =>
    setDrafts((old) => ({ ...old, [i]: { ...(old[i] ?? {}), [f]: v } }))

  async function saveRow(row: Record<string, unknown>, i: number) {
    const payload: Record<string, unknown> = {}
    if (row.id !== undefined) payload.id = row.id
    for (const f of fields) {
      if (inputType(f) === "number") {
        // 空串要跳过而不是转成 0:`Number("")` 是 0,用户清空价格本意是"没填",
        // 存成 0 会把它静默标成"免费"。非空但解析不出数字的也跳过,保留服务端原值。
        const raw = draft(i, f).trim()
        if (raw === "") continue
        const n = Number(raw)
        if (!Number.isNaN(n)) payload[f] = n
      } else {
        payload[f] = draft(i, f)
      }
    }
    await onSubmit(block.action ?? "save", payload)
  }

  return (
    <Card className="overflow-x-auto p-0">
      <Table>
        <TableHeader>
          <TableRow>
            {fields.map((f) => <TableHead key={f}>{f}</TableHead>)}
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
                const initial = cell(row[f])
                const type = inputType(f)
                return (
                  <TableCell key={f}>
                    <Input
                      type={type}
                      // 值是「草稿，回落到服务端原值」；清空后 placeholder 仍
                      // 显示原值，用户知道自己抹掉了什么。
                      value={draft(i, f)}
                      placeholder={initial}
                      className="min-w-32"
                      onChange={(e) => patch(i, f, e.target.value)}
                    />
                  </TableCell>
                )
              })}
              <TableCell className="text-right whitespace-nowrap">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={busy !== null}
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
  if (!page) return <p className="text-sm text-muted-foreground">加载中…</p>

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
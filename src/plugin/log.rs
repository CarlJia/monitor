//! 一次调用里的插件日志汇集点,支撑派发结果的 `detail`:有界、清洗、留最新。
//! 纯 std,不碰 wasmtime。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use super::host::truncate;

// ---------------------------------------------------------------------------
// 日志汇集(R16 的 detail)
// ---------------------------------------------------------------------------

/// 一次调用里留在日志汇集点里的最多条数。16 条足够说清「为什么失败」,又能
/// 让一个话多的插件在内存上封顶——超出的从**最旧**一端丢弃(失败原因一般在
/// 最新几行里)。
const LOG_LINES_MAX: usize = 16;

/// 单条日志的长度上限:200 字节。tg-notify 那句「kv 里没有 bot_token；请在
/// 面板的插件 KV 编辑器里填写」约 110 字节,必须整句留下;这条上限挡的是一次
/// `host_log` 就把整条 detail 占满。截断时会另加一个省略号,所以落进汇集点的
/// 单行最长 203 字节。
const LOG_LINE_MAX: usize = 200;

/// 一次调用里插件自己打出的日志,供面板在派发失败时显示「为什么」。
///
/// 记全部级别(0-3)不做过滤:插件可能返回非 0 却一条 warn 都没打,只掉 info 会
/// 让这里空着;而失败说明通常就是它打的最后一条,「留最新」已经把顺序问题解掉
/// 了。真要收窄,过滤点就在这里一个判断。
///
/// 为什么是 `Arc<Mutex<_>>` 而不是 Store 上的普通字段:Store 在 `call_on_event`
/// 里就没了,而调用方要在那之后读。更关键的是**超时路径**——`run_one` 放弃那个
/// 后台任务时,`call_on_event` 的返回值永远拿不到了,只有调用方事先持有的这个
/// Arc 才能看到"被放弃前插件打了什么"。
///
/// 有界:条数与单条长度都在这里截,不在读侧截。超时后那个被放弃的任务仍在往里
/// 写,读侧设限挡不住它继续长。
#[derive(Default)]
pub(crate) struct PluginLog {
    /// 最新在**后**。超 [`LOG_LINES_MAX`] 时从最旧一端丢。
    lines: VecDeque<String>,
    /// 被挤掉的条数,渲染时折算成一句「更早的 N 行已省略」。
    dropped: usize,
}

/// 会把面板「一行一项」的显示契约打破、或被用来伪装文本的不可见字符。
///
/// - `Cc`:换行、回车、制表等,能凭空多出一行或覆盖掉前一行;
/// - `Zl` / `Zp`(U+2028 / U+2029):同样是硬换行,但不在 `Cc` 里,CSS 的
///   `white-space: pre-line` 照样在这里断行;
/// - `Cf`:零宽字符与双向控制符,如 U+202E 能在一行之内把可见顺序颠倒。
///
/// 一并折成空格。detail 是给排障的操作员看的,保真让位于可读。
fn breaks_display(c: char) -> bool {
    // Cf 是逐码位的集合,std 没有 is_format;这里列的是对显示有影响的那部分
    // (零宽、双向、行/段分隔周边与哨兵字符),不追求覆盖 Cf 全集。
    const CF_RANGES: &[(u32, u32)] = &[
        (0x00AD, 0x00AD), // 软连字符
        (0x0600, 0x0605),
        (0x061C, 0x061C),
        (0x06DD, 0x06DD),
        (0x070F, 0x070F),
        (0x0890, 0x0891),
        (0x08E2, 0x08E2),
        (0x180E, 0x180E),
        (0x200B, 0x200F), // 零宽 + LRM/RLM + 双向嵌入标记
        (0x202A, 0x202E), // 双向嵌入与覆盖(LRE/RLE/PDF/LRO/RLO)
        (0x2060, 0x2064),
        (0x2066, 0x206F), // 双向隔离 + 已弃用的格式字符
        (0xFEFF, 0xFEFF),
        (0xFFF9, 0xFFFB),
        (0x110BD, 0x110BD),
        (0x110CD, 0x110CD),
        (0x13430, 0x1343F),
        (0x1BCA0, 0x1BCA3),
        (0x1D173, 0x1D17A),
        (0xE0001, 0xE0001),
        (0xE0020, 0xE007F),
    ];
    c.is_control()
        || matches!(c, '\u{2028}' | '\u{2029}')
        || CF_RANGES.iter().any(|&(lo, hi)| (lo..=hi).contains(&(c as u32)))
}

impl PluginLog {
    /// 记一条。不可见/换行类字符先换成空格(见 [`breaks_display`]):面板按行显示,
    /// 插件文案里的 `\n` 能凭空多出一行、`\r` 能覆盖掉前一行、`U+202E` 能颠倒一行
    /// 内的可见顺序。这个函数在 wasm 调用路径上,只做分配与截断,不会 panic。
    pub(super) fn push(&mut self, line: &str) {
        let cleaned: String = line.chars().map(|c| if breaks_display(c) { ' ' } else { c }).collect();
        self.lines.push_back(truncate(&cleaned, LOG_LINE_MAX));
        while self.lines.len() > LOG_LINES_MAX {
            self.lines.pop_front();
            self.dropped += 1;
        }
    }
}

/// 一次调用的日志汇集点。`Arc`:Store 的 user data 与调用方共享同一个。
pub(crate) type LogSink = Arc<Mutex<PluginLog>>;

/// 建一个空汇集点。每次调用一个——跨调用复用会把上一次的日志混进来。
pub(super) fn new_log_sink() -> LogSink {
    Arc::new(Mutex::new(PluginLog::default()))
}

/// 把汇集点渲染成 `DispatchEntry.detail`,留最新、不超过 `max` 字节。没有任何
/// 日志时返回 `None`(JSON 里就是 null)。
///
/// 调用方要保证 `max` 大于省略标记的预留(`NOTE_RESERVE`),否则为标记留出的预算
/// 会归零,`max` 就不再是硬上限——生产传的 `DETAIL_MAX` 远超这个下限。
///
/// 从最新往旧累积,所以下游那道上限不会吃掉失败原因;累积到放不下为止,再翻回
/// 时间顺序。省略标记放**开头**——面板的一行摘要取的是最后一行,那必须是插件
/// 最新打的那条。
pub(super) fn render_log(sink: &LogSink, max: usize) -> Option<String> {
    // 省略标记要占的位置,先留出来,免得加完标记反而超出 max。
    const NOTE_RESERVE: usize = 48;
    let log = sink.lock().unwrap_or_else(|e| e.into_inner());
    if log.lines.is_empty() {
        return None;
    }
    let budget = max.saturating_sub(NOTE_RESERVE);
    let mut kept: Vec<String> = Vec::new();
    let mut used = 0usize;
    for line in log.lines.iter().rev() {
        let cost = line.len() + 1;
        if used + cost > budget && !kept.is_empty() {
            break;
        }
        // 最新那条即使独占超预算也要留下——否则 detail 就空了;截到预算内,契约
        // 「不超过 max」才不会被一个很小的 max 打破。
        let line = if cost > budget { truncate(line, budget) } else { line.clone() };
        used += line.len() + 1;
        kept.push(line);
    }
    let omitted = log.dropped + (log.lines.len() - kept.len());
    kept.reverse();
    let body = kept.join("\n");
    if omitted == 0 {
        return Some(body);
    }
    Some(format!("…（更早的 {omitted} 行已省略）\n{body}"))
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// 汇集点有界:留最新 `LOG_LINES_MAX` 条,挤掉的记数,渲染时折算成省略标记。
    /// 留最新而不是最早——失败原因通常是插件最后打的那条。
    #[test]
    fn the_log_sink_keeps_the_newest_lines_and_counts_the_rest() {
        let logs = new_log_sink();
        {
            let mut log = logs.lock().unwrap();
            for i in 0..20 {
                log.push(&format!("line {i}"));
            }
            assert_eq!(log.lines.len(), LOG_LINES_MAX);
            assert_eq!(log.dropped, 4);
            assert_eq!(log.lines.front().unwrap(), "line 4", "最旧的 4 条被挤掉");
            assert_eq!(log.lines.back().unwrap(), "line 19");
        }
        let detail = render_log(&logs, 500).unwrap();
        assert!(detail.starts_with("…（更早的 4 行已省略）"), "实际: {detail}");
        assert!(detail.ends_with("line 19"), "最新一条要在最后(面板取最后一行),实际: {detail}");
    }

    /// 超过 `max` 时从最新往回装:最新那条必须完整留下,更早的折成省略标记。
    /// 空汇集点是 `None`(json 里的 null),不是空串。
    #[test]
    fn render_log_fits_the_budget_and_keeps_the_newest_line() {
        assert_eq!(render_log(&new_log_sink(), 500), None);
        let logs = new_log_sink();
        {
            let mut log = logs.lock().unwrap();
            for i in 0..6 {
                log.push(&format!("{}{i}", "x".repeat(90)));
            }
        }
        let detail = render_log(&logs, 300).unwrap();
        assert!(detail.len() <= 300, "不该超过 max,实际 {}", detail.len());
        assert!(detail.ends_with("5"), "最新的一条要在最后,实际: {detail}");
        assert!(detail.contains("已省略"), "放不下的更早行要折成省略,实际: {detail}");
        // max 比单条还小时同样守约:把最新那条截进去,而不是原样塞回来。
        let tight = render_log(&logs, 40).unwrap();
        assert!(tight.len() <= 40, "实际 {}: {tight}", tight.len());
    }

    /// 单条日志里的不可见字符折成空格:面板按行显示,插件文案里的 `\n` 能凭空
    /// 多出一行、`\r` 能覆盖掉前一行。只折 `Cc` 不够——`U+2028` 同样是硬换行
    /// 却不在 `Cc` 里,`U+202E` 能把一行内的可见顺序颠倒,两者都要挡。
    #[test]
    fn control_characters_in_a_log_line_are_neutralized() {
        let logs = new_log_sink();
        logs.lock().unwrap().push("first\r\nsecond");
        assert_eq!(render_log(&logs, 500).unwrap(), "first  second");
        let logs = new_log_sink();
        logs.lock().unwrap().push("evil\u{2028}line\u{202e}reversed\u{feff}");
        assert_eq!(render_log(&logs, 500).unwrap(), "evil line reversed ");
    }
}

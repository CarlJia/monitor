//! 公开备注渲染:把管理员写的 Markdown/HTML 源文本渲染为 HTML。
//!
//! 刻意**不做净化**:公开备注按受信任的管理员内容处理(见计划 R6/KTD2)。源里的
//! 裸 HTML 由 `pulldown-cmark` 原样透传。安全前提是「只有受信任的管理员能写
//! `public_remark`」;因此不引入 `ammonia`/`html5ever`(避免增大二进制)。

use pulldown_cmark::{html, Options, Parser};

/// 渲染公开备注源文本为 HTML。空(或仅空白)输入返回空串,便于上层按空处理
/// (前端据空值不渲染任何空框)。
pub fn render_public_remark(src: &str) -> String {
    if src.trim().is_empty() {
        return String::new();
    }
    // 常用扩展:表格、删除线、任务列表。裸 HTML 默认透传(push_html 原样写出
    // Html/InlineHtml 事件),这正是「支持 HTML 源」所需,也是不引入净化库的直接结果。
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts.insert(Options::ENABLE_TASKLISTS);
    let parser = Parser::new_ext(src, opts);
    let mut out = String::new();
    html::push_html(&mut out, parser);
    out
}

#[cfg(test)]
mod tests {
    use super::render_public_remark;

    #[test]
    fn renders_common_markdown() {
        let html = render_public_remark("**粗体** 和 [链接](https://example.com)");
        assert!(html.contains("<strong>粗体</strong>"), "{html}");
        assert!(html.contains("href=\"https://example.com\""), "{html}");
    }

    #[test]
    fn renders_lists_and_code_and_quote() {
        let html = render_public_remark("- a\n- b\n\n`code`\n\n> quote");
        assert!(html.contains("<ul>") && html.contains("<li>a</li>"), "{html}");
        assert!(html.contains("<code>code</code>"), "{html}");
        assert!(html.contains("<blockquote>"), "{html}");
    }

    #[test]
    fn renders_table() {
        let src = "| a | b |\n| - | - |\n| 1 | 2 |";
        let html = render_public_remark(src);
        assert!(html.contains("<table>") && html.contains("<td>1</td>"), "{html}");
    }

    #[test]
    fn passes_inline_html_through_unsanitized() {
        // 受信任管理员模型:裸 HTML 原样透传,不剥离(与净化模型相反)。
        let html = render_public_remark("<b>bold</b> <a href=\"/x\">x</a> <img src=\"/i.png\">");
        assert!(html.contains("<b>bold</b>"), "{html}");
        assert!(html.contains("<a href=\"/x\">x</a>"), "{html}");
        assert!(html.contains("<img src=\"/i.png\">"), "{html}");
    }

    #[test]
    fn empty_or_whitespace_source_renders_empty() {
        assert_eq!(render_public_remark(""), "");
        assert_eq!(render_public_remark("   \n\t "), "");
    }
}

//! Manifest(R7)。

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::notification_bus::Event;

use super::host::KV_KEY_MAX;

/// 宿主与插件之间的 ABI 版本。宿主大版本升级时递增;不匹配的插件在加载时被拒。
/// v2 起 ABI 只保留单一版本,不做 v1 兼容。
pub const ABI_VERSION: i64 = 2;

/// v2 的事件词表,单一来源是 [`Event::KNOWN`]:manifest 校验、db 的状态行
/// 与扫描循环读的都是同一组名字。v2 在宿主自身事件之外接受 `plugin_` 前缀的
/// 插件事件名(具体名不做白名单——事件由各插件运行时经 `emit_event` 发出,
/// 宿主无法预知全集,校验只查前缀与非空后缀)。manifest 声明订阅未来才有
/// 的宿主事件名仍会被拒:静默接受会让拼写错误无声失效,显式契约尽早暴露错误。
pub const KNOWN_EVENT_NAMES: [&str; 2] = Event::KNOWN;

/// `plugin_` 前缀:插件发出的事件名的强制前缀(KTD6)。
pub const PLUGIN_EVENT_PREFIX: &str = "plugin_";

/// manifest 的 `page` 声明:面板页面的标题。
#[derive(Debug, Clone, Deserialize)]
pub struct PageDecl {
    pub title: String,
}

/// manifest 的 `[[config]]` 声明:面板「配置」对话框要展示的一个 kv 字段。
///
/// 这是**面板的展示与预检依据,不是宿主对插件的契约**——真实派发从不检查它:
/// 后台事件旁边没有操作员,一个 400 也无处可给。所以 `required` 的含义只是
/// 「点『测试』前应该有值」;条件性才需要的字段(比如只有某种事件才用得上)留
/// `required = false`,让操作员自己判断。
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigDecl {
    /// kv 的 key,即 `plugin.<plugin_id>:<key>` 的右半边。
    pub key: String,
    /// 面板上显示的人话名字;缺省就只显示 key。
    pub label: Option<String>,
    /// 「测试」前是否必须有值。
    #[serde(default)]
    pub required: bool,
    /// 一句话说明该怎么填,面板显示在输入框下面。
    pub hint: Option<String>,
}

/// plugin.toml。字段与校验规则见 [`Manifest::parse`]。
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    /// 插件的稳定标识,反向域风格(如 `com.example.mailer`)。非空、不含 ':'
    /// ——它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符。
    pub plugin_id: String,
    /// 面板里显示的名字。
    pub name: String,
    /// 语义化版本。v2 只做非空校验。
    pub version: String,
    /// 必须等于 [`ABI_VERSION`]。
    pub abi_version: i64,
    /// 订阅的事件名:宿主自身事件必须是 [`KNOWN_EVENT_NAMES`] 之一,插件事件
    /// 以 `plugin_` 前缀声明。声明 tick/page/cleanup 的插件允许为空——它们
    /// 的工作面不在事件订阅上(财务插件依赖此放宽)。
    #[serde(default)]
    pub subscribes: Vec<String>,
    /// 每小时 housekeeping tick:声明后模块必须导出 `on_tick`(KTD4/KTD12)。
    #[serde(default)]
    pub tick: bool,
    /// 声明管理面板页面:模块必须导出 `render_page` 与 `on_action`(KTD5/KTD12)。
    pub page: Option<PageDecl>,
    /// 声明统一清理入口:模块必须导出 `on_cleanup`(KTD11/KTD12)。
    #[serde(default)]
    pub cleanup: bool,
    /// 包内 wasm 入口文件名。上传 API(U5)按它从包里取模块;运行期不再使用。
    #[serde(default = "default_wasm_entry")]
    pub wasm_entry: String,
    /// 面板「配置」对话框要展示的 kv 字段。见 [`ConfigDecl`]:只是面板的展示与
    /// 测试前预检,不构成工作面、也不参与真实派发。
    #[serde(default)]
    pub config: Vec<ConfigDecl>,
}

fn default_wasm_entry() -> String {
    "plugin.wasm".into()
}

impl Manifest {
    /// 解析并校验 manifest 文本。失败返回带原因的错误——上传 API(U5)把它转成
    /// 400,所以每条消息都要让插件作者知道改哪里。
    pub fn parse(toml_text: &str) -> Result<Self> {
        let m: Manifest = toml::from_str(toml_text).context("manifest 不是合法的 TOML")?;
        if m.plugin_id.trim().is_empty() {
            bail!("manifest.plugin_id 不能为空");
        }
        if m.plugin_id.contains(':') {
            bail!("manifest.plugin_id 不能包含 ':'(它是 kv 命名空间的分隔符)");
        }
        if m.name.trim().is_empty() {
            bail!("manifest.name 不能为空");
        }
        if m.version.trim().is_empty() {
            bail!("manifest.version 不能为空");
        }
        if m.abi_version != ABI_VERSION {
            bail!(
                "manifest.abi_version 必须为 {ABI_VERSION}(当前 {});v2 起不兼容 v1 插件,请用 v2 SDK 重build",
                m.abi_version
            );
        }
        if let Some(page) = &m.page {
            if page.title.trim().is_empty() {
                bail!("manifest.page.title 不能为空");
            }
        }
        // `[[config]]` 的 key 就是 kv 的 key,但宿主侧比面板的 kv 编辑器更严:
        // 额外拒绝首尾空白。面板写入的是原样 key,带空白的那一行与声明永远对不上,
        // 与其让「声明了却配不上」变成一个谜,不如在这里就挡住。
        let mut seen_keys = HashSet::new();
        for decl in &m.config {
            if decl.key.trim().is_empty() {
                bail!("manifest.config.key 不能为空");
            }
            if decl.key != decl.key.trim() {
                bail!("manifest.config.key `{}` 首尾不能有空白:面板写入的是原样 key", decl.key);
            }
            if decl.key.contains(':') {
                bail!("manifest.config.key 不能包含 ':'(它是 kv 命名空间的分隔符)");
            }
            if decl.key.len() > KV_KEY_MAX {
                bail!("manifest.config.key `{}` 超过 {KV_KEY_MAX} 字节的上限", decl.key);
            }
            // 重复会让面板为同一行 kv 渲染两个输入框。
            if !seen_keys.insert(decl.key.as_str()) {
                bail!("manifest.config 里 key `{}` 重复", decl.key);
            }
        }
        if m.subscribes.is_empty() && !m.tick && m.page.is_none() && !m.cleanup {
            bail!(
                "manifest.subscribes 至少要订阅一个事件;不订阅事件的插件要声明 tick、page 或 cleanup 之一(声明 [[config]] 不算工作面)"
            );
        }
        for event in &m.subscribes {
            let known = KNOWN_EVENT_NAMES.contains(&event.as_str());
            let plugin_event =
                event.len() > PLUGIN_EVENT_PREFIX.len() && event.starts_with(PLUGIN_EVENT_PREFIX);
            if !known && !plugin_event {
                bail!(
                    "manifest.subscribes 含未知事件 `{event}`;v2 支持的宿主事件: {},插件事件以 `{PLUGIN_EVENT_PREFIX}` 前缀声明",
                    KNOWN_EVENT_NAMES.join(", ")
                );
            }
        }
        Ok(m)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugin::test_util::MANIFEST;

    #[test]
    fn a_valid_manifest_parses() {
        let m = Manifest::parse(MANIFEST).unwrap();
        assert_eq!(m.plugin_id, "com.example.test");
        assert_eq!(m.subscribes, ["agent_offline", "plugin_expiry_soon"]);
        assert_eq!(m.wasm_entry, "plugin.wasm", "缺省的 wasm_entry");
        assert!(!m.tick);
        assert!(m.page.is_none());
        assert!(!m.cleanup);
        // 逐字段重写为非法值,每条都应带明确原因被拒;空串是控制组。
        for (field, value, needle) in [
            ("abi_version", "1", "abi_version"),
            ("abi_version", "3", "abi_version"),
            ("plugin_id", "\"a:b\"", "':'"),
            ("plugin_id", "\"\"", "不能为空"),
            ("version", "\"\"", "不能为空"),
            ("subscribes", "[\"expiryy_soon\"]", "未知事件"),
        ] {
            let edited = MANIFEST
                .lines()
                .map(|line| if line.starts_with(field) { format!("{field} = {value}") } else { line.into() })
                .collect::<Vec<_>>()
                .join("\n");
            let err = Manifest::parse(&edited).unwrap_err().to_string();
            assert!(err.contains(needle), "把 `{field}` 改成 {value} 应报 `{needle}`,实际: {err}");
        }
        // v1 拒载(v2 起单一版本,KTD1)。
        let v1 = MANIFEST.replace("abi_version = 2", "abi_version = 1");
        let err = Manifest::parse(&v1).unwrap_err().to_string();
        assert!(err.contains("不兼容 v1"), "实际: {err}");
    }

    /// `[[config]]` 的解析与校验:label/hint 可缺省、required 缺省为 false;
    /// 不能落库的 key 形状(空、首尾空白、含 ':'、超长、重复)逐条挡住——这些
    /// key 直接就是 kv 的 key,形状规则与面板的 kv 编辑器是同一套。
    #[test]
    fn config_declarations_are_parsed_and_validated() {
        let with = |block: &str| format!("{MANIFEST}\n{block}");
        let m = Manifest::parse(&with(
            "[[config]]\nkey = \"bot_token\"\nlabel = \"Bot Token\"\nrequired = true\nhint = \"向 @BotFather 申请\"\n",
        ))
        .unwrap();
        assert_eq!(m.config.len(), 1);
        assert_eq!(m.config[0].key, "bot_token");
        assert_eq!(m.config[0].label.as_deref(), Some("Bot Token"));
        assert!(m.config[0].required);
        assert_eq!(m.config[0].hint.as_deref(), Some("向 @BotFather 申请"));
        // 只有 key 是必需的:label/hint 缺省为 None,required 缺省为 false。
        let m = Manifest::parse(&with("[[config]]\nkey = \"chat_id\"\n")).unwrap();
        assert_eq!(m.config[0].label, None);
        assert_eq!(m.config[0].hint, None);
        assert!(!m.config[0].required);
        // 没有 [[config]] 的 manifest 得到空表(老插件不受影响)。
        assert!(Manifest::parse(MANIFEST).unwrap().config.is_empty());

        let over_long = format!("[[config]]\nkey = \"{}\"\n", "k".repeat(KV_KEY_MAX + 1));
        for (block, needle) in [
            ("[[config]]\nkey = \"\"\n", "不能为空"),
            ("[[config]]\nkey = \" bot\"\n", "空白"),
            ("[[config]]\nkey = \"a:b\"\n", "':'"),
            (over_long.as_str(), "上限"),
            ("[[config]]\nkey = \"t\"\n[[config]]\nkey = \"t\"\n", "重复"),
        ] {
            let err = Manifest::parse(&with(block)).unwrap_err().to_string();
            assert!(err.contains(needle), "`{block}` 应报 `{needle}`,实际: {err}");
        }
    }

    #[test]
    fn plugin_event_names_are_accepted_by_prefix() {
        let m = Manifest::parse(&format!("{MANIFEST}\ntick = true\n")).unwrap();
        assert!(m.tick);
        // 纯前缀(空后缀)不是合法事件名。
        let bad = MANIFEST.replace(
            "subscribes = [\"agent_offline\", \"plugin_expiry_soon\"]",
            "subscribes = [\"plugin_\"]",
        );
        assert!(Manifest::parse(&bad).is_err());
    }

    #[test]
    fn subscribes_may_be_empty_only_with_a_work_surface() {
        for decl in ["tick = true", "[page]\ntitle = \"X\"", "cleanup = true"] {
            let text = format!(
                "plugin_id = \"com.example.test\"\nname = \"t\"\nversion = \"1\"\nabi_version = 2\nsubscribes = []\n{decl}"
            );
            Manifest::parse(&text).unwrap_or_else(|e| panic!("声明 {decl} 应允许空 subscribes: {e}"));
        }
        let bare = "plugin_id = \"com.example.test\"\nname = \"t\"\nversion = \"1\"\nabi_version = 2\nsubscribes = []";
        let err = Manifest::parse(bare).unwrap_err().to_string();
        assert!(err.contains("至少"), "实际: {err}");
        // 声明 [[config]] 不算工作面:它只是面板的展示与预检,没有任何人调用这个
        // 插件。锁住这条,免得日后有人"顺手"把它算进去。
        let config_only = format!("{bare}\n[[config]]\nkey = \"bot_token\"\nrequired = true\n");
        let err = Manifest::parse(&config_only).unwrap_err().to_string();
        assert!(err.contains("至少"), "实际: {err}");
    }

    #[test]
    fn a_broken_manifest_is_not_toml() {
        assert!(Manifest::parse("plugin_id = ").is_err());
    }
}

//! Manifest(R7)。

use anyhow::{bail, Context, Result};
use serde::Deserialize;

use crate::notification_bus::Event;

/// 宿主与插件之间的 ABI 版本。宿主大版本升级时递增;不匹配的插件在加载时被拒。
pub const ABI_VERSION: i64 = 1;

/// v1 的事件词表,单一来源是 [`Event::KNOWN`]:manifest 校验、db 的状态行
/// 与扫描循环读的都是同一组名字。manifest 声明订阅未来才有的事件名会在这里
/// 被拒:静默接受会让拼写错误无声失效,显式契约尽早暴露错误(KTD2)。
pub const KNOWN_EVENT_NAMES: [&str; 3] = Event::KNOWN;

/// plugin.toml。字段与校验规则见 [`Manifest::parse`]。
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    /// 插件的稳定标识,反向域风格(如 `com.example.mailer`)。非空、不含 ':'
    /// ——它是 kv 命名空间 `plugin.<plugin_id>:<key>` 的分隔符。
    pub plugin_id: String,
    /// 面板里显示的名字。
    pub name: String,
    /// 语义化版本。v1 只做非空校验。
    pub version: String,
    /// 必须等于 [`ABI_VERSION`]。
    pub abi_version: i64,
    /// 订阅的事件名,必须是 [`KNOWN_EVENT_NAMES`] 之一,且至少一项——不订阅任何
    /// 事件的插件永远不会被派发,上传时拒绝而不是装一个死插件。
    pub subscribes: Vec<String>,
    /// 包内 wasm 入口文件名。上传 API(U5)按它从包里取模块;运行期不再使用。
    #[serde(default = "default_wasm_entry")]
    pub wasm_entry: String,
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
                "manifest.abi_version 必须为 {ABI_VERSION}(当前 {}),请用匹配的插件 SDK 重build",
                m.abi_version
            );
        }
        if m.subscribes.is_empty() {
            bail!("manifest.subscribes 至少要订阅一个事件");
        }
        for event in &m.subscribes {
            if !KNOWN_EVENT_NAMES.contains(&event.as_str()) {
                bail!(
                    "manifest.subscribes 含未知事件 `{event}`;v1 支持的事件: {}",
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
        assert_eq!(m.subscribes, ["expiry_soon", "agent_offline"]);
        assert_eq!(m.wasm_entry, "plugin.wasm", "缺省的 wasm_entry");
        // 逐字段重写为非法值,每条都应带明确原因被拒;空串是控制组。
        for (field, value, needle) in [
            ("abi_version", "2", "abi_version"),
            ("plugin_id", "\"a:b\"", "':'"),
            ("plugin_id", "\"\"", "不能为空"),
            ("version", "\"\"", "不能为空"),
            ("subscribes", "[]", "至少"),
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
    }

    #[test]
    fn a_broken_manifest_is_not_toml() {
        assert!(Manifest::parse("plugin_id = ").is_err());
    }
}

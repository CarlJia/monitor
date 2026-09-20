//! Manifest(R7)。

use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use serde_json::Value;

use crate::notification_bus::Event;

use super::host::{kv_key_problem, KvKeyProblem, KV_KEY_MAX};

/// 宿主与插件之间的 ABI 版本。宿主大版本升级时递增;不匹配的插件在加载时被拒。
/// v2 起 ABI 只保留单一版本,不做 v1 兼容。
pub const ABI_VERSION: i64 = 2;

/// v2 的事件词表,单一来源是 [`Event::KNOWN`]:manifest 校验、db 的状态行
/// 与扫描循环读的都是同一组名字。v2 在宿主自身事件之外接受 `plugin_` 前缀的
/// 插件事件名(具体名不做白名单——事件由各插件运行时经 `emit_event` 发出,
/// 宿主无法预知全集,校验只查前缀与非空后缀)。manifest 声明订阅未来才有
/// 的宿主事件名仍会被拒:静默接受会让拼写错误无声失效,显式契约尽早暴露错误。
pub const KNOWN_EVENT_NAMES: [&str; Event::KNOWN.len()] = Event::KNOWN;

/// `plugin_` 前缀:插件发出的事件名的强制前缀(KTD6)。宿主函数 `emit_event`
/// 用同一个常数判(它的实现在 `monitor-plugin-contract` 里),所以定义在契约
/// crate,这里只 re-export:manifest 校验与宿主函数不可能各认一个前缀。
pub use monitor_plugin_contract::constants::PLUGIN_EVENT_PREFIX;

/// kv 值的体量上限。`[[kv]]` 的 `default` 是面板预填的文案,它最终要么进 kv
/// 行、要么与 kv 行等价,所以按同一个上限挡——两处不同的话,作者能写下一个
/// 面板显示得出来、却永远存不进去的默认值。
use monitor_plugin_contract::constants::KV_VALUE_MAX;

/// `[[kv]]` 的条数上限。64 项已远超真实插件(tg-notify 只声明 2 项);这条挡的
/// 不是错误配置,而是「64 KiB 的 manifest 塞进上千个声明」——那些声明会被插件
/// 列表、每次「测试」的必填预检和配置对话框各自放大一遍。
const KV_DECL_MAX: usize = 64;

/// `subscribes` 的条数上限。v2 的宿主事件只有四个,其余靠 `plugin_` 前缀自由
/// 命名;32 条同样远超需要,挡的是同一类放大(每次派发都要逐条比对)。
const SUBSCRIBE_MAX: usize = 32;

/// manifest 的 `page` 声明:面板页面的标题。
#[derive(Debug, Clone, Deserialize)]
pub struct PageDecl {
    pub title: String,
}

/// manifest 的 `[[kv]]` 声明:面板「配置」对话框要展示的一个 kv 字段。
///
/// 这是**面板的展示与预检依据,不是宿主对插件的契约**——真实派发从不检查它:
/// 后台事件旁边没有操作员,一个 400 也无处可给。所以 `required` 的含义只是
/// 「点『测试』前应该有值」;条件性才需要的字段(比如只有某种事件才用得上)留
/// `required = false`,让操作员自己判断。
///
/// v2.x 起另有两个纯展示字段:`kind`(`type`,单行还是多行)与 `default`
/// (没配值时对话框里预填什么)。两者都只影响面板,插件运行时读到的仍然是 kv。
#[derive(Debug, Clone, Deserialize)]
pub struct KvDecl {
    /// kv 的 key,即 `plugin.<plugin_id>:<key>` 的右半边。
    pub key: String,
    /// 面板上显示的人话名字;缺省就只显示 key。
    pub label: Option<String>,
    /// 「测试」前是否必须有值。
    #[serde(default)]
    pub required: bool,
    /// 一句话说明该怎么填,面板显示在输入框下面。
    pub hint: Option<String>,
    /// 编辑形态:`text`(缺省)或 `textarea`。面板据此决定用单行输入框还是
    /// 多行编辑器——多行是通用能力,面板不知道哪个字段是「模板」。
    ///
    /// 存成 String 而不是枚举:非法值要报出**是哪个 kv key** 写错了,而 serde
    /// 的 `unknown variant` 只说位置、不说 key。
    #[serde(rename = "type", default = "default_kv_kind")]
    pub kind: String,
    /// 该 kv 没有值(或值为空白)时,面板在编辑器里预填的文案。插件运行时的
    /// 内置默认值仍写在插件自己的代码里——这份是给操作员看的基准,好让他「改
    /// 一个字」而不是从空白写起。
    #[serde(default)]
    pub default: Option<String>,
}

/// `KvDecl::kind` 的缺省值:单行。
fn default_kv_kind() -> String {
    "text".into()
}

/// manifest 的 `[[sample]]` 声明:一条**插件事件**的测试样例载荷。
///
/// 宿主的「测试」按插件声明的 `subscribes` 逐条合成事件;宿主自身事件有真实
/// 结构可造,`plugin_` 前缀的事件宿主一无所知,只能回放插件在这里给的样例。
/// 说清事件长什么样是插件的知识,不是宿主的(KTD4)。
#[derive(Debug, Clone, Deserialize)]
pub struct SampleDecl {
    /// 事件名,必须出现在本 manifest 的 `subscribes` 里。
    pub name: String,
    /// 一份 JSON 对象,顶层不能带 `type`——事件名由宿主注入,与
    /// `Event::Plugin` 的序列化规则同一份语义。
    pub payload: String,
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
    /// 面板「配置」对话框要展示的 kv 字段。见 [`KvDecl`]:只是面板的展示与
    /// 测试前预检,不构成工作面、也不参与真实派发。
    #[serde(default)]
    pub kv: Vec<KvDecl>,
    /// 「测试」要回放的插件事件样例。见 [`SampleDecl`]:只有 `plugin_` 前缀的
    /// 订阅项需要它——宿主自身事件由宿主自己造得出真实结构。
    ///
    /// TOML 里是 `[[sample]]`(单数,与 `[[kv]]` 同形),字段名取复数读起来更像
    /// 一个表:这一处是 manifest 里唯一「键名与字段名不同形」的地方,rename 写
    /// 明它,免得日后有人照着字段名去写 `[[samples]]`——那会被静默忽略(未知键
    /// 一律忽略,这正是老宿主能读新 manifest 的原因),而插件作者只会看到
    /// 「样例没生效」。
    #[serde(rename = "sample", default)]
    pub samples: Vec<SampleDecl>,
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
        // key 的形状规则单点在 [`kv_key_problem`],这里只把它折成 manifest 的
        // 文案;「一张表里不能重复」是本处独有的约束(面板与 kv 命名空间都没有
        // 这个概念),仍留在这儿。
        if m.kv.len() > KV_DECL_MAX {
            bail!("manifest.kv 最多声明 {KV_DECL_MAX} 项(当前 {})", m.kv.len());
        }
        let mut seen_keys = HashSet::new();
        for decl in &m.kv {
            match kv_key_problem(&decl.key) {
                None => {}
                Some(KvKeyProblem::Empty) => bail!("manifest.kv.key 不能为空"),
                Some(KvKeyProblem::Padded) => {
                    bail!("manifest.kv.key `{}` 首尾不能有空白(面板与接口写入的是原样 key)", decl.key)
                }
                Some(KvKeyProblem::Colon) => {
                    bail!("manifest.kv.key 不能包含 ':'(它是 kv 命名空间的分隔符)")
                }
                Some(KvKeyProblem::TooLong) => {
                    bail!("manifest.kv.key `{}` 超过 {KV_KEY_MAX} 字节的上限", decl.key)
                }
            }
            // 重复会让面板为同一行 kv 渲染两个输入框。
            if !seen_keys.insert(decl.key.as_str()) {
                bail!("manifest.kv 里 key `{}` 重复", decl.key);
            }
            // 编辑形态:词表小且封闭。写错一个词在上传时就是显式错误,而不是在
            // 面板上默默退化成单行框——操作员会以为是自己填错了。
            if decl.kind != "text" && decl.kind != "textarea" {
                bail!(
                    "manifest.kv `{}` 的 type 只能是 `text` 或 `textarea`(当前 `{}`)",
                    decl.key,
                    decl.kind
                );
            }
            // 必填与默认值互斥:两者并存会让「这个框是空的」既表示缺配置、又表示
            // 用默认值,测试前的必填预检就说不清该不该拦。
            if decl.required && decl.default.is_some() {
                bail!(
                    "manifest.kv `{}` 不能同时声明 required 与 default:必填的字段没有默认值可用",
                    decl.key
                );
            }
            // 默认值最终要么原样进 kv 行、要么与 kv 行等价,所以按 kv 值的同一上限
            // 挡——两处不同的话,作者能写下一个面板显示得出来、却永远存不进去的值。
            if let Some(default) = &decl.default {
                if default.len() > KV_VALUE_MAX {
                    bail!(
                        "manifest.kv `{}` 的 default 超过 {KV_VALUE_MAX} 字节的上限(当前 {})",
                        decl.key,
                        default.len()
                    );
                }
            }
        }
        if m.subscribes.is_empty() && !m.tick && m.page.is_none() && !m.cleanup {
            bail!(
                "manifest.subscribes 至少要订阅一个事件;不订阅事件的插件要声明 tick、page 或 cleanup 之一(声明 [[kv]] 不算工作面)"
            );
        }
        if m.subscribes.len() > SUBSCRIBE_MAX {
            bail!(
                "manifest.subscribes 最多订阅 {SUBSCRIBE_MAX} 个事件(当前 {});宿主事件只有 {} 个,其余靠自己发",
                m.subscribes.len(),
                KNOWN_EVENT_NAMES.join(", ")
            );
        }
        let mut seen_events = HashSet::new();
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
            // 重复项在真实派发里只算一次(registry 用 `.any()` 判订阅),但「测试」是
            // 按这张表逐条派发的——留着重复项,点一次测试就会把同一个事件真发两遍。
            if !seen_events.insert(event.as_str()) {
                bail!("manifest.subscribes 里事件 `{event}` 重复");
            }
        }
        // 「测试」要回放的样例:只有订阅了的事件才可能被派发到,给没订阅的事件
        // 写样例只会让人以为测过了。payload 的顶层不许带 `type`——事件名由宿主
        // 注入,与 `Event::Plugin` 的序列化规则是同一份语义。
        let mut seen_samples = HashSet::new();
        for sample in &m.samples {
            if !m.subscribes.contains(&sample.name) {
                bail!(
                    "manifest.sample 的事件 `{}` 不在 subscribes 里;样例只能给本插件订阅的事件用",
                    sample.name
                );
            }
            // 宿主自身事件由宿主自己造得出真实结构(见 api_plugins::synthetic_events),
            // 样例根本不会被查询。放着不拒就是一份永远不生效的死配置,作者还会以为
            // 测试时的节点名/时间是他给的。
            if KNOWN_EVENT_NAMES.contains(&sample.name.as_str()) {
                bail!(
                    "manifest.sample 的事件 `{}` 是宿主自身事件;样例只对 `{PLUGIN_EVENT_PREFIX}` 前缀的插件事件有意义",
                    sample.name
                );
            }
            if !seen_samples.insert(sample.name.as_str()) {
                bail!("manifest.sample 里事件 `{}` 重复", sample.name);
            }
            match serde_json::from_str::<Value>(&sample.payload) {
                Ok(Value::Object(fields)) => {
                    if fields.contains_key("type") {
                        bail!(
                            "manifest.sample `{}` 的 payload 不能带 `type`:事件名由宿主注入",
                            sample.name
                        );
                    }
                }
                _ => bail!(
                    "manifest.sample `{}` 的 payload 必须是一个 JSON 对象,收到 `{}`",
                    sample.name,
                    sample.payload
                ),
            }
        }
        Ok(m)
    }
}

/// 这个包能否替换已装的那份：只有**版本更高**才允许。
///
/// 按点分段、逐段当数字比大小（`1.10` > `1.9`），缺的段按 0（`1.2` 与
/// `1.2.0` 是同一个版本，不算升级）。认不出的写法（某段没有数字前缀）一律
/// 当 0，于是「比不出高下」的两次上传等同版本、一并拒绝——宁可让作者把版本
/// 号写清楚，也不让一个手滑的包默默盖掉线上那份（数据在，代码换了，最难查）。
pub fn is_newer_version(candidate: &str, installed: &str) -> bool {
    version_segments(candidate) > version_segments(installed)
}

/// 版本 → 可比较的数字段。尾部多余的 0 去掉，让 `1.2` 与 `1.2.0` 相等。
fn version_segments(version: &str) -> Vec<u64> {
    let mut segments: Vec<u64> = version
        .split('.')
        .map(|segment| {
            let digits: String = segment.trim().chars().take_while(char::is_ascii_digit).collect();
            // 溢出与「没有数字」都落到 0：这一段比不出大小，就不许它撑起一次升级。
            digits.parse().unwrap_or(0)
        })
        .collect();
    while segments.len() > 1 && segments.last() == Some(&0) {
        segments.pop();
    }
    segments
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

    /// `[[kv]]` 的解析与校验:label/hint 可缺省、required 缺省为 false;
    /// 不能落库的 key 形状(空、首尾空白、含 ':'、超长、重复)逐条挡住——这些
    /// key 直接就是 kv 的 key,形状规则与面板的 kv 编辑器是同一套。
    #[test]
    fn kv_declarations_are_parsed_and_validated() {
        let with = |block: &str| format!("{MANIFEST}\n{block}");
        let m = Manifest::parse(&with(
            "[[kv]]\nkey = \"bot_token\"\nlabel = \"Bot Token\"\nrequired = true\nhint = \"向 @BotFather 申请\"\n",
        ))
        .unwrap();
        assert_eq!(m.kv.len(), 1);
        assert_eq!(m.kv[0].key, "bot_token");
        assert_eq!(m.kv[0].label.as_deref(), Some("Bot Token"));
        assert!(m.kv[0].required);
        assert_eq!(m.kv[0].hint.as_deref(), Some("向 @BotFather 申请"));
        // 只有 key 是必需的:label/hint 缺省为 None,required 缺省为 false。
        let m = Manifest::parse(&with("[[kv]]\nkey = \"chat_id\"\n")).unwrap();
        assert_eq!(m.kv[0].label, None);
        assert_eq!(m.kv[0].hint, None);
        assert!(!m.kv[0].required);
        // 没有 [[kv]] 的 manifest 得到空表(老插件不受影响)。
        assert!(Manifest::parse(MANIFEST).unwrap().kv.is_empty());

        let over_long = format!("[[kv]]\nkey = \"{}\"\n", "k".repeat(KV_KEY_MAX + 1));
        for (block, needle) in [
            ("[[kv]]\nkey = \"\"\n", "不能为空"),
            ("[[kv]]\nkey = \" bot\"\n", "空白"),
            ("[[kv]]\nkey = \"a:b\"\n", "':'"),
            (over_long.as_str(), "上限"),
            ("[[kv]]\nkey = \"t\"\n[[kv]]\nkey = \"t\"\n", "重复"),
        ] {
            let err = Manifest::parse(&with(block)).unwrap_err().to_string();
            assert!(err.contains(needle), "`{block}` 应报 `{needle}`,实际: {err}");
        }
    }

    /// `[[kv]]` 的编辑形态与默认值,以及 `[[sample]]` 样例:面板据此选控件、预填
    /// 内置文案;样例只对订阅的事件有意义(宿主对别的插件的事件字段一无所知)。
    #[test]
    fn kv_editing_hints_and_test_samples_are_validated() {
        let with = |block: &str| format!("{MANIFEST}\n{block}");
        // 缺省:单行编辑器、没有默认值——老插件的 manifest 一个字段都不用改。
        let m = Manifest::parse(&with("[[kv]]\nkey = \"chat_id\"\n")).unwrap();
        assert_eq!(m.kv[0].kind, "text");
        assert_eq!(m.kv[0].default, None);
        // 声明多行与默认值。
        let m = Manifest::parse(&with(
            "[[kv]]\nkey = \"tpl\"\ntype = \"textarea\"\ndefault = \"⏰ {name} 到期\"\n",
        ))
        .unwrap();
        assert_eq!(m.kv[0].kind, "textarea");
        assert_eq!(m.kv[0].default.as_deref(), Some("⏰ {name} 到期"));
        // 样例:name 在订阅里才收;没有 [[sample]] 的老 manifest 得到空表。
        let m = Manifest::parse(&with(
            "[[sample]]\nname = \"plugin_expiry_soon\"\npayload = '{\"node_id\":0}'\n",
        ))
        .unwrap();
        assert_eq!(m.samples.len(), 1);
        assert_eq!(m.samples[0].name, "plugin_expiry_soon");
        assert!(Manifest::parse(MANIFEST).unwrap().samples.is_empty());

        let over_long = format!("[[kv]]\nkey = \"tpl\"\ndefault = \"{}\"\n", "x".repeat(KV_VALUE_MAX + 1));
        for (block, needle) in [
            ("[[kv]]\nkey = \"tpl\"\ntype = \"multiline\"\n", "type"),
            ("[[kv]]\nkey = \"bot_token\"\nrequired = true\ndefault = \"x\"\n", "required"),
            (over_long.as_str(), "上限"),
            // agent_online 不在 MANIFEST 的 subscribes 里(它只有 agent_offline)。
            ("[[sample]]\nname = \"agent_online\"\npayload = \"{}\"\n", "subscribes"),
            // 宿主自身事件不在插件的「样本」范围里——宿主自己造得出真实结构,样例不会生效。
            ("[[sample]]\nname = \"agent_offline\"\npayload = \"{}\"\n", "宿主"),
            ("[[sample]]\nname = \"plugin_expiry_soon\"\npayload = \"[]\"\n", "对象"),
            ("[[sample]]\nname = \"plugin_expiry_soon\"\npayload = '{\"type\":\"x\"}'\n", "type"),
            (
                "[[sample]]\nname = \"plugin_expiry_soon\"\npayload = \"{}\"\n[[sample]]\nname = \"plugin_expiry_soon\"\npayload = \"{}\"\n",
                "重复",
            ),
        ] {
            let err = Manifest::parse(&with(block)).unwrap_err().to_string();
            assert!(err.contains(needle), "`{block}` 应报 `{needle}`,实际: {err}");
        }
    }

    /// 两份清单都有条数上限:挡的是「64 KiB 的 manifest 塞进上千个声明,再被
    /// 插件列表、必填预检与配置对话框各自放大一遍」。刚好到上限仍应通过。
    #[test]
    fn manifest_lists_are_bounded() {
        // tick 提供工作面,好让 subscribes 可以为空。
        let base =
            "plugin_id = \"com.example.test\"\nname = \"t\"\nversion = \"1\"\nabi_version = 2\ntick = true\n";
        let kv = |n: usize| (0..n).map(|i| format!("[[kv]]\nkey = \"k{i}\"\n")).collect::<String>();
        let at_cap = Manifest::parse(&format!("{base}subscribes = []\n{}", kv(KV_DECL_MAX))).unwrap();
        assert_eq!(at_cap.kv.len(), KV_DECL_MAX, "刚好到上限应当通过");
        let err = Manifest::parse(&format!("{base}subscribes = []\n{}", kv(KV_DECL_MAX + 1)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("最多声明"), "实际: {err}");

        let events = |n: usize| (0..n).map(|i| format!("\"plugin_ev{i}\"")).collect::<Vec<_>>().join(", ");
        assert!(
            Manifest::parse(&format!("{base}subscribes = [{}]", events(SUBSCRIBE_MAX))).is_ok(),
            "刚好到上限应当通过"
        );
        let err = Manifest::parse(&format!("{base}subscribes = [{}]", events(SUBSCRIBE_MAX + 1)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("最多订阅"), "实际: {err}");
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
        // 声明 [[kv]] 不算工作面:它只是面板的展示与预检,没有任何人调用这个
        // 插件。锁住这条,免得日后有人"顺手"把它算进去。
        let kv_only = format!("{bare}\n[[kv]]\nkey = \"bot_token\"\nrequired = true\n");
        let err = Manifest::parse(&kv_only).unwrap_err().to_string();
        assert!(err.contains("至少"), "实际: {err}");
    }

    /// `subscribes` 里的重复项在真实派发里只算一次(registry 用 `.any()` 判订阅),
    /// 但「测试」按整表遍历、逐条派发——留个重复项,点一次测试就会把同一个事件真
    /// 发两遍。上传时拦住比在运行时让操作员追消息来源更好查。
    #[test]
    fn duplicate_subscribes_entries_are_rejected() {
        let text = "plugin_id = \"com.example.dup\"\nname = \"t\"\nversion = \"1\"\nabi_version = 2\n\
                   subscribes = [\"agent_offline\", \"agent_online\", \"agent_offline\"]\n";
        let err = Manifest::parse(text).unwrap_err().to_string();
        assert!(err.contains("agent_offline") && err.contains("重复"), "应点名重复的 agent_offline,实际:{err}");
    }

    #[test]
    fn a_broken_manifest_is_not_toml() {
        assert!(Manifest::parse("plugin_id = ").is_err());
    }

    #[test]
    fn only_a_higher_version_may_replace() {
        // 逐段数字比大小，不是字符串比大小。
        assert!(is_newer_version("1.10.0", "1.9.0"));
        assert!(is_newer_version("2.0.0", "1.99.99"));
        assert!(is_newer_version("1.2.1", "1.2"));
        // 缺段按 0：1.2 与 1.2.0 是同一个版本，不是升级。
        assert!(!is_newer_version("1.2", "1.2.0"));
        assert!(!is_newer_version("1.2.0", "1.2"));
        // 同版本与降级都换不动。
        assert!(!is_newer_version("1.0.0", "1.0.0"));
        assert!(!is_newer_version("0.9.0", "1.0.0"));
        // 认不出的写法（无数字前缀、空串）当 0 处理：比不出高下就不许替换。
        assert!(!is_newer_version("", "1.0.0"));
        assert!(!is_newer_version("abc", "1.0.0"));
        assert!(!is_newer_version("1.0.0-rc2", "1.0.0"), "后缀段按数字前缀比，同段不算升级");
        // 反过来，已装版本比不出数字时，正常写的版本就算升级——那种行本来也
        // 只可能是手工塞进去的。
        assert!(is_newer_version("1.0.0", "abc"));
    }
}

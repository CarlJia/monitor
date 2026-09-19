//! 契约替身的 http 后端:可插拔,默认「全拒」。

use std::sync::Mutex;
use std::time::Instant;

use crate::error_codes::ERR_SSRF;
use crate::host::HttpMethod;

/// 契约测试可换的 http 出口。
///
/// 返回 `Ok(响应体)` 或 `Err(错误码)`:错误码与真宿主的 `plugin_http_fetch`
/// 一套(+非 2xx 是 -5、网络失败 -4、策略拒绝 -9 等)。
pub trait ContractHttp: Send + Sync {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<Vec<u8>, i32>;
}

/// 默认实现:**拒绝一切请求**,返回 SSRF 拒绝码(-9)。
///
/// 默认安全(KTD4):契约测试必须显式装上 [`MockHttp`] 才走得通有出网的路径,
/// 谁也不会因为"忘了拦"而让插件在 CI 里真的打到公网。
pub struct NoopHttp;

impl ContractHttp for NoopHttp {
    fn request(
        &self,
        _method: HttpMethod,
        url: &str,
        _body: Option<Vec<u8>>,
        _deadline: Instant,
    ) -> Result<Vec<u8>, i32> {
        tracing::warn!(url = %url, "契约替身未装 http 后端,按 SSRF 拒绝");
        Err(ERR_SSRF)
    }
}

/// 一次被记录的请求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpCall {
    pub method: HttpMethod,
    pub url: String,
    pub body: Option<Vec<u8>>,
}

/// 显式构造响应的 http 替身,同时记下调用现场供断言。
pub struct MockHttp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    calls: Mutex<Vec<HttpCall>>,
}

impl MockHttp {
    /// 固定回一个状态码与响应体。`Err(-5)` 在非 2xx 时给出,与真宿主一致。
    pub fn respond_with(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self { status, headers: Vec::new(), body: body.into(), calls: Mutex::new(Vec::new()) }
    }

    /// 同上,再带上响应头(替身目前不回传头,记下来供断言用)。
    pub fn respond_with_headers(
        status: u16,
        headers: Vec<(String, String)>,
        body: impl Into<Vec<u8>>,
    ) -> Self {
        Self { status, headers, body: body.into(), calls: Mutex::new(Vec::new()) }
    }

    pub fn status(&self) -> u16 {
        self.status
    }

    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// 这个替身收到过的请求,按发生顺序。
    pub fn calls(&self) -> Vec<HttpCall> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

impl ContractHttp for MockHttp {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        _deadline: Instant,
    ) -> Result<Vec<u8>, i32> {
        self.calls.lock().unwrap_or_else(|e| e.into_inner()).push(HttpCall {
            method,
            url: url.to_owned(),
            body: body.clone(),
        });
        if (200..300).contains(&self.status) {
            Ok(self.body.clone())
        } else {
            Err(-5)
        }
    }
}

/// `Arc<T>` 也是 `ContractHttp`:让用例留住一份句柄去断言调用现场
/// (`with_http` 取 `Box<dyn ContractHttp>`,裸放一个 `MockHttp` 进去就拿不回来
/// 看 `calls()` 了——门的价值一半在"插件**真的发了**那个请求")。
impl<T: ContractHttp + ?Sized> ContractHttp for std::sync::Arc<T> {
    fn request(
        &self,
        method: HttpMethod,
        url: &str,
        body: Option<Vec<u8>>,
        deadline: Instant,
    ) -> Result<Vec<u8>, i32> {
        (**self).request(method, url, body, deadline)
    }
}

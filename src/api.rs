//! The panel and public-status HTTP surface.

use axum::extract::multipart::MultipartError;
use axum::extract::rejection::JsonRejection;
use axum::extract::ws::{Message, Utf8Bytes, WebSocket, WebSocketUpgrade};
use axum::extract::{ConnectInfo, FromRequestParts, Multipart, Path, Query, State};
use axum::http::request::Parts;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{Local, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::debug;

use crate::agent_ws::Agent;
use crate::auth::{
    authed, client_ip, current_session, hash_password, issue_session, issued_at, random_token, with_cookies,
};
use crate::db::{Node, NodePatch, PingTask, PluginRow, Traffic, TrafficPatch};
use crate::notification_bus::Event;
use crate::plugin::{self, Manifest};
use crate::{agent_ws, App, Shared};

/// Present only on requests carrying a valid session. Handlers taking it cannot
/// be reached unauthenticated, so the check cannot be omitted.
pub struct Admin;

impl FromRequestParts<Shared> for Admin {
    type Rejection = StatusCode;

    async fn from_request_parts(parts: &mut Parts, app: &Shared) -> Result<Self, Self::Rejection> {
        if authed(app, &parts.headers) {
            Ok(Admin)
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

fn fail(e: impl std::fmt::Display) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

fn bad(message: &str) -> Response {
    (StatusCode::BAD_REQUEST, message.to_owned()).into_response()
}

// ---- read paths, shared between the panel and the public page ----

/// Everything a report may expose under `metrics` on the public page: the agent
/// contract minus the raw kernel counters, which are a wire-protocol detail
/// disclosing a machine's entire lifetime traffic, plus the four figures the hub
/// folds in itself. The panel sees the report as it arrived.
pub(crate) const PUBLIC_METRICS: [&str; 18] = [
    "uptime",
    "cpu",
    "load",
    "mem_total",
    "mem_used",
    "swap_total",
    "swap_used",
    "disk_total",
    "disk_used",
    "net_rx",
    "net_tx",
    "tcp",
    "udp",
    "procs",
    "total_rx",
    "total_tx",
    "month_rx",
    "month_tx",
];

/// One node as the UI consumes it: stored config, live metrics and the hub's
/// accumulated traffic in a single object.
fn node_view(node: &Node, current: Option<&Agent>, traffic: &Traffic, full: bool) -> Value {
    // The three capacities arrive twice: once in `Facts`, sent at the handshake
    // and stored, and again in every `Metrics`. A machine that gains a disk while
    // the agent is running -- the agent re-reads its mount table every sample so
    // that it appears -- then has a stored figure that is stale until the next
    // reconnect, possibly days away. Using the report while a node is connected
    // keeps every consumer of this view on one number: the card reads the live
    // metrics and the detail page reads these, and they previously showed the
    // same machine two different capacities. Offline, the stored figure is all
    // there is. No floor is applied: a host whose swap has just been disabled
    // reports zero and means it. A node connected but not yet reporting holds
    // `Null`, where `get` returns nothing and the stored figure stands.
    let live = |key: &str, stored: i64| {
        current.and_then(|a| a.metrics.get(key).and_then(serde_json::Value::as_i64)).unwrap_or(stored)
    };
    let mut view = json!({
        "id": node.id,
        "name": node.name,
        // A country rather than an address: it indicates which region a node sits
        // in, which is what a status page conveys, without locating it. The
        // address it was derived from remains behind the panel.
        "country": node.country,
        "sort": node.sort,
        "public": node.public,
        "online": current.is_some(),
        // The live entry while connected, the stored one afterwards. Zero means
        // connected but not yet reporting, which is not a timestamp, so it falls
        // back to the stored value and "offline since" survives the gap.
        "last_seen": current.map(|a| a.last_seen).filter(|t| *t > 0).unwrap_or(node.last_seen),
        "metrics": current.map(|a| a.metrics.clone()).unwrap_or(Value::Null),
        "os": node.os,
        "kernel": node.kernel,
        "arch": node.arch,
        "virt": node.virt,
        "cpu_name": node.cpu_name,
        "cpu_cores": node.cpu_cores,
        "mem_total": live("mem_total", node.mem_total),
        "swap_total": live("swap_total", node.swap_total),
        "disk_total": live("disk_total", node.disk_total),
        "agent_version": node.agent_version,
        "price": node.price,
        "currency": node.currency,
        "billing_cycle": node.billing_cycle,
        "expires_at": node.expires_at,
        "traffic_limit": node.traffic_limit,
        "traffic_mode": node.traffic_mode,
        "traffic_reset_day": node.traffic_reset_day,
        "total_rx": traffic.total_rx,
        "total_tx": traffic.total_tx,
        "month_rx": traffic.month_rx,
        "month_tx": traffic.month_tx,
        "month_start": traffic.month_start,
        // Of the same nature as the month and lifetime figures beside it, which
        // the public page already shows, so this one is public as well.
        "day_rx": traffic.day_rx,
        "day_tx": traffic.day_tx,
    });
    // An allowlist rather than a denylist: the agent ships from its own
    // repository, so a field added there would otherwise reach anonymous visitors
    // the day it is released. No address, hostname or note may ever do so.
    if !full {
        if let Some(m) = view["metrics"].as_object_mut() {
            m.retain(|k, _| PUBLIC_METRICS.contains(&k.as_str()));
        }
    }
    // Address, private notes and the token never leave the panel. The token is
    // included so the install command can be displayed without reissuing it.
    if full {
        view["hostname"] = json!(node.hostname);
        view["ip"] = json!(node.ip);
        view["ipv4"] = json!(node.ipv4);
        view["ipv6"] = json!(node.ipv6);
        view["remark"] = json!(node.remark);
        view["token"] = json!(node.token);
    }
    view
}

fn visible_nodes(app: &App, full: bool) -> Result<Vec<Value>, anyhow::Error> {
    // One traffic query and one lock for the whole list, since this is what every
    // visitor to the public page loads.
    let nodes = app.db.nodes()?;
    let traffic = app.db.all_traffic();
    let agents = app.agents.read().unwrap_or_else(|e| e.into_inner());
    let none = Traffic::default();
    Ok(nodes
        .iter()
        .filter(|n| full || n.public)
        .map(|n| node_view(n, agents.get(&n.id), traffic.get(&n.id).unwrap_or(&none), full))
        .collect())
}

pub async fn nodes(State(app): State<Shared>, headers: HeaderMap) -> Response {
    let full = authed(&app, &headers);
    if !full && !app.public_page() {
        return (StatusCode::UNAUTHORIZED, "sign-in required").into_response();
    }
    // The same rendered frame the browser streams receive, for the same reason:
    // otherwise every visitor would rebuild every node's row against the
    // connection the agents write through.
    ([(axum::http::header::CONTENT_TYPE, "application/json")], live_snapshot(&app, full).as_str().to_owned())
        .into_response()
}

#[derive(Deserialize)]
pub struct Window {
    #[serde(default = "default_hours")]
    hours: i64,
    /// How many points the caller can draw. Absent means the full budget.
    points: Option<i64>,
    /// Which half the caller will draw, `metrics` or `ping`. Each tab draws one,
    /// and the other accounted for a third to two thirds of every response. Absent
    /// means both.
    series: Option<String>,
}

fn default_hours() -> i64 {
    6
}

/// How many history windows are built concurrently.
///
/// `PUBLIC_HOURS` bounds what one request costs; this bounds how many may run,
/// closing the same gap `main::RELAY_GATE` and `auth::PASSWORD_GATE` close on
/// the other two paths an anonymous caller can make expensive. This is the most
/// expensive of the three: every request holds the single connection the agents
/// report through for its entire scan, measured at 118 ms for a week of four
/// probes and growing with `retention_days`. Without a gate, 120 requests from
/// one machine took the panel's own node list from 1 ms to 2.8 s.
///
/// Four, because the requests serialise on that one connection regardless: a
/// fifth in flight buys no throughput and merely places another scan ahead of
/// the next agent report. What the number actually sets is how long that wait
/// can become -- four at roughly 120 ms is half a second -- while leaving room
/// for several people opening charts simultaneously.
///
/// Refused rather than queued, as in `auth`: a queue admits the same flood,
/// merely later.
///
/// **This gate is ineffective without the `spawn_blocking` below.** The body of
/// this handler never awaits, so a permit taken and dropped within it is held
/// only while a worker thread is actually running the handler -- at most one per
/// worker, three on this hub. Measured at eight: 120 concurrent requests, zero
/// refusals, the panel still at 14 s. This is the same constraint
/// `PASSWORD_CHECKS` is sized against, approached from the other side: not a
/// value too high for the machine, but a handler that cannot hold more permits
/// than the machine has threads. Moving the scan off the runtime is what makes
/// "in flight" meaningful, and is what every other heavy query here already
/// does.
const HISTORY_SLOTS: usize = 4;
static HISTORY_GATE: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(HISTORY_SLOTS);

pub async fn metrics(
    State(app): State<Shared>,
    headers: HeaderMap,
    Path(id): Path<i64>,
    Query(w): Query<Window>,
) -> Response {
    let full = authed(&app, &headers);
    if !readable(&app, full, id) {
        return (StatusCode::UNAUTHORIZED, "sign-in required").into_response();
    }
    // After the two point lookups above, so an unauthorised caller is told so
    // rather than asked to retry later.
    let Ok(_permit) = HISTORY_GATE.try_acquire() else {
        return (StatusCode::SERVICE_UNAVAILABLE, "too many history queries in flight, try again")
            .into_response();
    };
    let hours = w.hours.clamp(1, if full { ADMIN_HOURS } else { PUBLIC_HOURS });
    let since = Utc::now().timestamp() - hours * 3_600;
    let step = sample_step(hours, w.points);
    let wants = |name: &str| w.series.as_deref().is_none_or(|s| s == name);
    let (want_metrics, want_ping) = (wants("metrics"), wants("ping"));
    // Off the runtime, for the reason given in `db_stats` below: this reads every
    // probe result the node has retained within the window and holds the
    // connection the agents report through throughout. That route is behind
    // `Admin` and cheaper than this one, which anyone can reach.
    //
    // It is also what makes the gate above effective: the permit is held across
    // an await, so exactly four callers are inside it at once rather than however
    // many worker threads happen to exist.
    let built = tokio::task::spawn_blocking(move || {
        // Probe names accompany the samples they label, so the page needs no
        // second request. Names only: targets and assignments remain behind
        // `Admin`. Skipped when probes were not requested, since the resources tab
        // has nothing to label and this costs a turn at the write connection.
        let probes =
            if want_ping { app.db.ping_task_names(id).unwrap_or_else(|_| json!({})) } else { json!({}) };
        let metrics = if want_metrics { app.db.metrics(id, since, step)? } else { vec![] };
        // `loss` is per probe across the whole window, alongside the per-bucket
        // `loss` on the rows. Both are required and neither replaces the other:
        // the row figure is what a tooltip reads, while the window figure is the
        // only one that can be accurate, since the denominators it divides by are
        // gone by the time the rows are built. Additive, so a theme unaware of it
        // continues to work.
        let (ping, loss) =
            if want_ping { app.db.ping_records(id, since, step)? } else { (vec![], json!({})) };
        anyhow::Ok(json!({"metrics": metrics, "ping": ping, "probes": probes, "loss": loss}))
    })
    .await;
    match built.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        Ok(body) => Json(body).into_response(),
        Err(e) => fail(e),
    }
}

/// Widest history window each audience may request.
///
/// The thinning below bounds the response, not the scan behind it: `hours=2160`
/// returns 320 rows after reading every probe result the node has retained. At a
/// month of retention that is 224 ms holding the single write connection the
/// agents report through, growing with `retention_days`.
///
/// The public ceiling is a week because that is the widest chart the themes
/// draw, so nothing in use is lost. The panel retains the quarter year, being
/// one signed-in operator rather than an anonymous caller.
const PUBLIC_HOURS: i64 = 24 * 7;
const ADMIN_HOURS: i64 = 24 * 90;

/// Seconds between the samples a window is drawn from.
///
/// Thinning exists for what the screen cannot draw rather than as a convention:
/// where the samples fit, every one is sent. A chart of a hundred points reads
/// as a hundred samples taken, which for a probe is a claim about the network.
/// Whole minutes, matching the grid the metric rows sit on.
///
/// `points` is what the caller reports it can draw, and can only lower the
/// budget: `SAMPLES` is the hub's ceiling rather than the caller's, set at a day
/// of minutes so the widest charted probe window returns intact.
// ponytail: the budget is per series, so a response is SAMPLES × (1 + probes) --
// bounded by how many probes the admin created, not by the caller. Four probes
// at a day is ~90 kB gzipped; if that list ever grows long, scale SAMPLES by
// the probe count.
fn sample_step(hours: i64, points: Option<i64>) -> i64 {
    const SAMPLES: i64 = 1_440;
    let budget = points.unwrap_or(SAMPLES).clamp(60, SAMPLES);
    // Rounded up, or the budget would not be one: a window that does not divide
    // evenly would keep the finer step and exceed it. `i64::div_ceil` is still
    // unstable, and both operands are positive here.
    60 * ((hours * 60 + budget - 1) / budget).max(1)
}

/// Guards a per-node read: the panel sees everything, while the public page sees
/// only nodes explicitly published. `full` is the caller's own `authed`, passed
/// in because the handler also needs it for the window ceiling.
fn readable(app: &App, full: bool, id: i64) -> bool {
    full || (app.public_page() && app.db.node(id).ok().flatten().is_some_and(|n| n.public))
}

/// Per-connection read buffer for both WebSocket surfaces. The 128 KiB default
/// would be tens of megabytes across a few hundred agents, for frames a few
/// hundred bytes long.
pub const SOCKET_BUFFER: usize = 4 * 1024;

/// Largest frame either socket accepts, matching the 64 KiB cap on the HTTP
/// body. That limit is a tower layer and never applies here, where the default
/// ceiling is 64 MiB -- reachable with a node's own token, for content that is
/// stored and then served to every viewer of the public page.
pub const MAX_FRAME: usize = 64 * 1024;

/// How long one rendered snapshot is reused. Just under the push interval, so
/// every tick rebuilds once and no viewer receives a stale frame twice.
const SNAPSHOT_TTL_MS: i64 = 1_900;

/// The payload every browser stream sends, built at most once per tick however
/// many tabs are watching: the public page is anonymous, so a per-connection
/// build would make viewer count a multiplier on database work. Two slots,
/// because the admin view carries fields the public one must never expose.
fn live_snapshot(app: &App, full: bool) -> Utf8Bytes {
    let now = Utc::now().timestamp_millis();
    let slot = usize::from(full);
    let mut cache = app.snapshot.lock().unwrap_or_else(|e| e.into_inner());
    // A cached frame's age must be non-negative. A wall clock can step backwards
    // -- NTP correcting a fresh boot -- and against a bare upper bound the
    // resulting negative reads as young, pinning the panel to a stale frame until
    // real time catches up.
    if (0..SNAPSHOT_TTL_MS).contains(&now.saturating_sub(cache[slot].0)) {
        return cache[slot].1.clone();
    }
    let nodes = visible_nodes(app, full).unwrap_or_default();
    // `admin` is included so the panel's first fetch and its stream share one
    // cached frame.
    let payload = Utf8Bytes::from(json!({"nodes": nodes, "admin": full}).to_string());
    cache[slot] = (now, payload.clone());
    payload
}

/// Drops the cached frames so the next push rebuilds. Without it a node just
/// added in the panel would disappear from the list until the frame expires.
fn invalidate_snapshot(app: &App) {
    for slot in app.snapshot.lock().unwrap_or_else(|e| e.into_inner()).iter_mut() {
        slot.0 = 0;
    }
}

/// What one tick of a browser stream may send: the admin frame while the session
/// that opened it remains live, the public frame while the status page remains
/// open to anonymous callers, and nothing once either ceases to hold.
///
/// Both are checked every tick rather than at the handshake alone, because a
/// socket outlives both answers. The admin frame carries every node's token in
/// the clear, so one outliving its session would distribute credentials that
/// survive revocation -- the same gap `reset_token` closes on the agent side by
/// dropping its sender. The public frame is what an operator withdraws by
/// switching the status page off, and a socket opened a minute earlier would
/// continue sending it for as long as the tab stayed open: `live_ws` refuses new
/// anonymous connections from that moment and `nodes` answers them 401, leaving
/// this the only remaining route. Whatever the handshake tested must be tested
/// here as well.
fn stream_audience(app: &App, session: Option<&str>) -> Option<bool> {
    match session {
        Some(hash) => app.db.session_valid(hash).then_some(true),
        None => app.public_page().then_some(false),
    }
}

/// Live stream for the browser. Each connection runs its own timer -- simpler to
/// reason about than a fan-out channel -- over a shared snapshot, so a timer
/// costs no more than a send.
pub async fn live_ws(State(app): State<Shared>, headers: HeaderMap, upgrade: WebSocketUpgrade) -> Response {
    // The digest rather than the result: signing out must reach a stream already
    // running, and only the row it names can report whether it has.
    let session = current_session(&headers).filter(|hash| app.db.session_valid(hash));
    if session.is_none() && !app.public_page() {
        return (StatusCode::UNAUTHORIZED, "sign-in required").into_response();
    }
    upgrade
        .read_buffer_size(SOCKET_BUFFER)
        .max_message_size(MAX_FRAME)
        .on_upgrade(move |socket| stream_live(app, socket, session))
}

async fn stream_live(app: Shared, mut socket: WebSocket, session: Option<String>) {
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(2));
    loop {
        ticker.tick().await;
        // Closed rather than downgraded to the public frame, which would leave the
        // panel rendering a list with every admin field missing. The close allows
        // a client to re-query /api/me and determine its current state.
        let Some(full) = stream_audience(&app, session.as_deref()) else { break };
        if socket.send(Message::Text(live_snapshot(&app, full))).await.is_err() {
            break;
        }
    }
}

// ---- panel write paths ----

/// Names all three causes. A reverse proxy that does not preserve Host forwards
/// its own upstream address, which is an IP and therefore never an https domain
/// entry, while the admin reading this is already on the domain -- so the first
/// clause alone would point them in the wrong direction. The third is `--site`,
/// the one input to this decision that nothing about the request reveals: a hub
/// started with `--site https://198.51.100.7` refuses every provisioning call
/// from an otherwise valid https domain entry. `main` warns about that at
/// startup; this is for whoever reads the panel rather than the journal.
const PROVISIONING_DENIED: &str = "请通过 HTTPS 域名访问面板后添加或安装节点；\
     如果已经是域名访问，检查反向代理是否透传了 Host 与 X-Forwarded-Proto（见 README 的反代配置）；\
     两者都没问题就检查 hub 的启动参数 --site，它必须是 https:// 加域名，不能是 IP、不能带路径";

pub(crate) fn https_domain(site: &str) -> Option<reqwest::Url> {
    let url = reqwest::Url::parse(site).ok()?;
    (url.scheme() == "https"
        && url.domain().is_some_and(|d| d != "localhost" && !d.ends_with(".localhost"))
        && url.username().is_empty()
        && url.password().is_none()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none())
    .then_some(url)
}

/// Host and the proxy's scheme describe this request; --site must not turn an IP
/// entry point into a domain entry point. The listener remains behind the trusted
/// reverse proxy, which must preserve Host and set X-Forwarded-Proto.
///
/// Every refusal names which half failed. Without that, a proxy configured with a
/// bare `proxy_pass` -- nginx then forwards `Host: 127.0.0.1:28080`, as does
/// Apache under its default `ProxyPreserveHost Off` -- is indistinguishable from
/// a genuine IP entry point: provisioning stops working across an upgrade, the
/// message implicates the address bar, and nothing records the header actually
/// responsible.
fn provisioning_allowed(app: &App, headers: &HeaderMap) -> bool {
    let Some(host) = headers.get(header::HOST).and_then(|v| v.to_str().ok()) else {
        debug!("provisioning refused: the request carries no readable Host header");
        return false;
    };
    let forwarded = crate::forwarded_proto(headers);
    let https = forwarded.map_or_else(|| app.site.starts_with("https://"), |scheme| scheme == "https");
    if !https || (!app.site.is_empty() && https_domain(&app.site).is_none()) {
        debug!(
            "provisioning refused: not an https domain entry (X-Forwarded-Proto={forwarded:?}, --site={:?}); \
             a TLS-terminating proxy has to send X-Forwarded-Proto: https",
            app.site
        );
        return false;
    }
    let Some(url) = https_domain(&format!("https://{host}")) else {
        debug!(
            "provisioning refused: Host {host:?} is not an https domain entry; a reverse proxy that does \
             not preserve Host sends its own upstream address here -- nginx needs \
             `proxy_set_header Host $host`, Apache `ProxyPreserveHost On`"
        );
        return false;
    };
    let expected = url.origin().ascii_serialization();
    let allowed =
        headers.get(header::ORIGIN).is_none_or(|origin| origin.to_str().ok() == Some(expected.as_str()));
    if !allowed {
        debug!("provisioning refused: Origin {:?} is not {expected}", headers.get(header::ORIGIN));
    }
    allowed
}

/// Range and sign limits every stored node must satisfy, or the reason it does
/// not. Shared because both writers must enforce them: the create path formerly
/// accepted a whole `Node` unchecked, leaving the values the update path refuses
/// reachable by another route, and an out-of-range reset day remained harmless
/// only because `period_start` clamps what it reads.
fn node_limits(reset_day: Option<u32>, price: Option<f64>, limit: Option<i64>) -> Option<&'static str> {
    if reset_day.is_some_and(|d| !(1..=31).contains(&d)) {
        return Some("reset day must be from 1 to 31");
    }
    if price.is_some_and(|v| !v.is_finite() || v < 0.0) || limit.is_some_and(|v| v < 0) {
        return Some("price and traffic limit must be non-negative");
    }
    None
}

pub async fn me(State(app): State<Shared>, headers: HeaderMap) -> Json<Value> {
    Json(json!({
        "authed": authed(&app, &headers),
        "github": app.db.get("github_client_id").is_some_and(|v| !v.is_empty()),
        "site_name": app.db.get("site_name").unwrap_or_else(|| "Monitor".into()),
        "public_page": app.public_page(),
        "can_provision": provisioning_allowed(&app, &headers),
        // The hub's own public URL when one was given, which is what belongs in an
        // install command and in the OAuth callback -- not whichever address this
        // browser used, which behind a proxy may be a loopback port. Empty by
        // default, in which case the browser's address is the only one available
        // and the panel falls back to its own origin.
        "site": app.site,
    }))
}

pub async fn create_node(
    _: Admin,
    State(app): State<Shared>,
    headers: HeaderMap,
    body: Result<Json<Node>, JsonRejection>,
) -> Response {
    if !provisioning_allowed(&app, &headers) {
        return (StatusCode::FORBIDDEN, PROVISIONING_DENIED).into_response();
    }
    let Ok(Json(mut node)) = body else { return bad("invalid node") };
    if node.name.trim().is_empty() {
        return bad("name is required");
    }
    if let Some(message) =
        node_limits(Some(node.traffic_reset_day), Some(node.price), Some(node.traffic_limit))
    {
        return bad(message);
    }
    node.name = node.name.trim().to_owned();
    let token = random_token();
    match app.db.create_node(&node, &token) {
        // Usable immediately: the install command is readable from the node list,
        // so adding and deploying require no reissue in between.
        Ok(id) => {
            invalidate_snapshot(&app);
            Json(json!({"id": id})).into_response()
        }
        Err(e) => fail(e),
    }
}

// ---- automatic registration ----

/// How long a registration window stays open.
///
/// Provisioning a batch of machines takes minutes, and the window expires on its
/// own rather than depending on someone returning to close it.
const REGISTER_WINDOW: i64 = 3600;

/// How many nodes one window may register.
///
/// Without it, whoever holds the key for the hour could fill the node table. A
/// hundred is well beyond a plausible batch and well short of a problem.
const REGISTER_LIMIT: i64 = 100;

/// Exchanges a registration key for a node token, so a batch of machines can be
/// installed with one command rather than one panel visit each.
///
/// No session stands behind this route: the caller is `install.sh` on a machine
/// that has never contacted the hub. A key issued by the panel, valid only within
/// [`REGISTER_WINDOW`], serves in place of a session.
///
/// One request costs two setting reads, a `COUNT` and an `INSERT`. It makes no
/// outbound request, and the router's 64 KiB body limit bounds the name.
pub async fn agent_register(
    State(app): State<Shared>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
    // Plain text in, plain text out. The caller is a shell script, as with this
    // route's neighbours: `/install.sh` and `/agent/{arch}` return a script and a
    // binary. A bare token is one `$(curl ...)` away, requiring no JSON parser in
    // a POSIX `sh`.
    name: String,
) -> Response {
    if !provisioning_allowed(&app, &headers) {
        return (StatusCode::FORBIDDEN, PROVISIONING_DENIED).into_response();
    }
    let ip = client_ip(&headers, peer.ip());
    // Counted separately from the sign-in page: a batch install started with a
    // stale key is a misconfigured deploy rather than an attack on the panel, and
    // a shared counter would lock the operator out of their own hub for LOCKOUT.
    if app.registrations.locked(ip) {
        return (StatusCode::TOO_MANY_REQUESTS, "too many attempts, try again later").into_response();
    }
    // One answer for both "no window is open" and "that key is wrong": the
    // difference is only useful to someone who has neither.
    let closed = || (StatusCode::FORBIDDEN, "registration is closed").into_response();
    let until = app.db.get("register_until").and_then(|v| v.parse::<i64>().ok()).unwrap_or(0);
    let Some(key) = app.db.get("register_key").filter(|k| !k.is_empty() && Utc::now().timestamp() < until)
    else {
        return closed();
    };
    if agent_ws::bearer(&headers) != Some(key.as_str()) {
        // Only an incorrect key counts against the address. With the window
        // closed there is no secret to guess, and counting then would let anyone
        // lock an address they name in `X-Forwarded-For` out of the sign-in page.
        app.registrations.record_failure(ip);
        return closed();
    }
    match app.db.nodes_created_since(until - REGISTER_WINDOW) {
        Ok(n) if n >= REGISTER_LIMIT => {
            return (StatusCode::FORBIDDEN, "this window has registered enough nodes").into_response()
        }
        Err(e) => return fail(e),
        Ok(_) => {}
    }

    // The name comes from a machine not yet vouched for: control characters would
    // break the panel's rows, and the length must be bounded. `chars()` rather
    // than bytes, so the cut falls on a character boundary.
    let name: String = name.trim().chars().filter(|c| !c.is_control()).take(64).collect();
    let name = if name.is_empty() { "unnamed".to_owned() } else { name };
    // Field defaults live in `Node`'s serde attributes and nowhere else.
    // `Node::default()` is a different set of values -- private, reset day 0 --
    // and a node registered here must match one added through the panel.
    let node = match serde_json::from_value::<Node>(json!({ "name": name })) {
        Ok(node) => node,
        Err(e) => return fail(e),
    };
    let token = random_token();
    match app.db.create_node(&node, &token) {
        Ok(_) => {
            app.registrations.clear(ip);
            invalidate_snapshot(&app);
            token.into_response()
        }
        Err(e) => fail(e),
    }
}

/// Opens a registration window with a fresh key. Any previous key stops working
/// the moment this returns.
pub async fn open_register(_: Admin, State(app): State<Shared>, headers: HeaderMap) -> Response {
    if !provisioning_allowed(&app, &headers) {
        return (StatusCode::FORBIDDEN, PROVISIONING_DENIED).into_response();
    }
    let key = random_token();
    let until = (Utc::now().timestamp() + REGISTER_WINDOW).to_string();
    match app.db.set("register_key", &key).and_then(|()| app.db.set("register_until", &until)) {
        Ok(()) => Json(json!({"register_key": key, "register_until": until})).into_response(),
        Err(e) => fail(e),
    }
}

/// Closes the window early, before the hour elapses.
pub async fn close_register(_: Admin, State(app): State<Shared>) -> Response {
    match app.db.set("register_key", "").and_then(|()| app.db.set("register_until", "0")) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => fail(e),
    }
}

pub async fn update_node(
    _: Admin,
    State(app): State<Shared>,
    Path(id): Path<i64>,
    body: Result<Json<NodePatch>, JsonRejection>,
) -> Response {
    let Ok(Json(mut node)) = body else { return bad("invalid node") };
    if let Some(name) = &mut node.name {
        *name = name.trim().to_owned();
        if name.is_empty() {
            return bad("name is required");
        }
    }
    if let Some(message) = node_limits(node.traffic_reset_day, node.price, node.traffic_limit) {
        return bad(message);
    }
    match app.db.update_node(id, &node) {
        Ok(()) => {
            invalidate_snapshot(&app);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => fail(e),
    }
}

#[derive(Deserialize)]
pub struct NodeOrder {
    ids: Vec<i64>,
}

/// The list must name every node exactly once, checked inside the transaction
/// that renumbers rather than here: re-reading the node list first would only
/// race the write it guards.
pub async fn reorder_nodes(_: Admin, State(app): State<Shared>, Json(order): Json<NodeOrder>) -> Response {
    match app.db.reorder_nodes(&order.ids) {
        Ok(()) => {
            invalidate_snapshot(&app);
            Json(json!({"ok": true})).into_response()
        }
        // Every failure here indicates a malformed list from the caller.
        Err(e) => bad(&e.to_string()),
    }
}

pub async fn delete_node(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    // The token is checked only at the handshake, so deleting the row does not
    // end a connection already open on it; dropping the sender does. Without
    // this the agent would keep reporting under an id SQLite reassigns to the
    // next node created, which would then appear online on another node's
    // metrics. The same reasoning applies in `reset_token` below.
    app.agents.write().unwrap_or_else(|e| e.into_inner()).remove(&id);
    match app.db.delete_node(id) {
        Ok(()) => {
            invalidate_snapshot(&app);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => fail(e),
    }
}

/// Issues a fresh token, invalidating the old one immediately.
///
/// Always an explicit action: rotate a token believed to have leaked, then
/// reinstall the agent. Reading the install command does not pass through here.
pub async fn reset_token(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let token = random_token();
    let updated = app.db.node(id).map(|n| n.is_some()).unwrap_or(false);
    if !updated {
        return (StatusCode::NOT_FOUND, "no such node").into_response();
    }
    // The token is checked only at the handshake, so a session opened with the
    // old one would continue reporting. Dropping the sender ends that loop; the
    // agent reconnects and is refused. Its own teardown leaves the entry
    // untouched, because the session tag no longer matches.
    app.agents.write().unwrap_or_else(|e| e.into_inner()).remove(&id);
    // The token is part of the admin frame, which would otherwise continue to
    // display an install command for the credential just retired.
    invalidate_snapshot(&app);
    match app.db.reset_token(id, &token) {
        // The token alone: the panel builds the command, and one place needs to
        // know its form.
        Ok(()) => Json(json!({"token": token})).into_response(),
        Err(e) => fail(e),
    }
}

pub async fn patch_traffic(
    _: Admin,
    State(app): State<Shared>,
    Path(id): Path<i64>,
    Json(p): Json<TrafficPatch>,
) -> Response {
    if [p.total_rx, p.total_tx, p.month_rx, p.month_tx].into_iter().flatten().any(|v| v < 0) {
        return bad("traffic must be non-negative");
    }
    match app.db.set_traffic(id, &p) {
        Ok(()) => {
            invalidate_snapshot(&app);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => fail(e),
    }
}

pub async fn ping_tasks(_: Admin, State(app): State<Shared>) -> Response {
    match app.db.ping_tasks() {
        Ok(tasks) => Json(json!({"tasks": tasks})).into_response(),
        Err(e) => fail(e),
    }
}

/// A probe target the agent can resolve: `host:port`, with an IPv6 literal
/// bracketed as a URL writes one.
///
/// A bare `contains(':')` admitted three forms that never connect: a bare IPv6
/// address, which is all colons; `:443` with no host; and `host:` with no port.
/// The agent's `lookup_host` errors on each, `tcp_ping` returns -1, and the chart
/// draws a probe at 100% loss indefinitely with nothing in any log identifying
/// the target as the cause.
fn valid_target(target: &str) -> bool {
    let (host, port) = match target.strip_prefix('[') {
        Some(rest) => match rest.split_once("]:") {
            Some(pair) => pair,
            None => return false,
        },
        // Unbracketed, so the last colon is the port separator; anything still
        // containing a colon is an IPv6 address that required brackets.
        None => match target.rsplit_once(':') {
            Some((host, port)) if !host.contains(':') => (host, port),
            _ => return false,
        },
    };
    !host.is_empty() && port.parse::<u16>().is_ok_and(|p| p > 0)
}

pub async fn save_ping_task(_: Admin, State(app): State<Shared>, Json(mut task): Json<PingTask>) -> Response {
    // Trimmed into the stored value rather than a discarded copy: what reaches
    // the agent is `task.target`, and a trailing space from a paste passes
    // `valid_target` while `lookup_host` rejects the stored string outright,
    // leaving the probe reporting -1 indefinitely. The name is trimmed for the
    // same reason, as it travels to the public page as a chart label.
    task.name = task.name.trim().to_owned();
    task.target = task.target.trim().to_owned();
    if task.name.is_empty() || task.target.is_empty() {
        return bad("name and target are required");
    }
    // A TCP probe requires an explicit port; a bare host would silently never
    // connect.
    if !valid_target(&task.target) {
        return bad("target must be host:port, for example 1.1.1.1:443 or [2606:4700:4700::1111]:443");
    }
    // Refused rather than clamped, for the reason `setting_error` gives for
    // `retention_days`: the agent clamps this again on arrival, so an
    // out-of-range value never fails but silently becomes a different number
    // while the panel still displays what was entered. Below the floor that
    // number is 5 seconds, the fastest probe available, run by every node the
    // task is assigned to; the panel reaches 0 simply by having its interval
    // field cleared.
    if !(5..=3_600).contains(&task.interval) {
        return bad("interval must be from 5 to 3600 seconds");
    }
    match app.db.save_ping_task(&task) {
        Ok(id) => {
            agent_ws::push_ping_tasks(&app);
            Json(json!({"id": id})).into_response()
        }
        // Every failure here originates with the caller: a node id that does not
        // exist, or more probes on one node than the agent will run. The same
        // reasoning as `reorder_nodes`.
        Err(e) => bad(&e.to_string()),
    }
}

pub async fn delete_ping_task(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    match app.db.delete_ping_task(id) {
        Ok(()) => {
            agent_ws::push_ping_tasks(&app);
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => fail(e),
    }
}

/// Settings the panel may read. Secrets are deliberately excluded: the client can
/// set the GitHub secret but never read it back.
const READABLE_SETTINGS: &[&str] = &[
    "site_name",
    "public_page",
    "github_client_id",
    "github_allowed_users",
    "retention_days",
    "theme",
    "github_proxy",
];

// ---- the database itself ----

/// The largest single request the two upload routes accept, and the reason they
/// sit outside the router's 64 KiB body limit. It is twice the 4 MiB the panel
/// sends, so the chunk size remains the panel's concern alone and requires no
/// negotiated handshake.
///
/// **This, not the two ceilings below, is what a reverse proxy must pass.** A
/// backup of any size arrives 4 MiB at a time, so `client_max_body_size` no
/// longer tracks the size of the database.
pub const MAX_CHUNK: usize = 8 * 1024 * 1024;

/// Whole-file ceilings, one per route, checked against the declared `total` on
/// the first request rather than by counting bytes as they arrive, so an
/// oversized upload is refused before a byte is sent.
///
/// The backup ceiling is set where it is because restoring holds the connection
/// every read and write passes through: at the measured ~40 MB/s that is roughly
/// 6.5 seconds during which the panel and the public page also wait. Database
/// sizes reachable with a few hundred nodes sit two orders of magnitude below
/// it.
pub const MAX_RESTORE: u64 = 256 * 1024 * 1024;
pub const MAX_THEME: u64 = 32 * 1024 * 1024;

/// One request of an upload: `total` is the whole file, `offset` where this piece
/// belongs within it.
///
/// There is no upload id, session or server-side bookkeeping: the state of an
/// upload is the length of the file on disk. A piece continues an upload only if
/// it begins exactly where the last ended, `offset = 0` truncates whatever an
/// interrupted attempt left behind, and nothing is ever left to collect.
#[derive(Deserialize)]
pub struct Chunk {
    offset: u64,
    total: u64,
}

/// Appends one piece to `path`, returning the file's length afterwards; the
/// caller compares that against `total` to determine completion.
///
/// A piece lands whole or not at all -- a failure truncates back to where it
/// began -- so retrying one always aligns on the same offset.
///
/// ponytail: strictly sequential, one round trip per chunk. Concurrent pieces
/// would require pwrite, a commit step and a hash to prove there are no gaps,
/// and would save about a second on a 6.7 MB backup.
async fn receive(path: &str, chunk: &Chunk, max: u64, body: axum::body::Body) -> Result<u64, anyhow::Error> {
    if chunk.total == 0 || chunk.total > max {
        anyhow::bail!("文件必须在 1 字节到 {} MiB 之间", max / 1024 / 1024);
    }
    if chunk.offset > chunk.total {
        anyhow::bail!("分片位置越过了文件末尾");
    }

    let mut options = std::fs::OpenOptions::new();
    // Only the first piece may create the file, and it truncates: whatever an
    // interrupted upload left behind is overwritten rather than accumulated.
    if chunk.offset == 0 {
        options.write(true).create(true).truncate(true);
    } else {
        options.append(true);
    }
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = match options.open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            anyhow::bail!("这次上传已经不在了，请从头开始")
        }
        Err(e) => return Err(e.into()),
    };

    let already = file.metadata()?.len();
    if already != chunk.offset {
        anyhow::bail!("分片接不上：已经收到 {already} 字节，这一片却从 {} 开始", chunk.offset);
    }

    match append(&mut file, chunk, body).await {
        Ok(received) => Ok(chunk.offset + received),
        Err(e) => {
            // Undo a partially written piece so a retry aligns again.
            let _ = file.set_len(chunk.offset);
            Err(e)
        }
    }
}

/// Streams one request body onto the end of `file`. The byte count is checked
/// here as well as by the route's body limit: these are the only paths on the hub
/// that write a caller's bytes to disk, so they do not depend on a layer that
/// could be reordered away.
async fn append(
    file: &mut std::fs::File,
    chunk: &Chunk,
    body: axum::body::Body,
) -> Result<u64, anyhow::Error> {
    use std::io::Write;
    use std::pin::Pin;

    let mut stream = body.into_data_stream();
    let mut received = 0u64;
    while let Some(piece) =
        std::future::poll_fn(|cx| futures_core::Stream::poll_next(Pin::new(&mut stream), cx)).await
    {
        let piece = piece?;
        received += piece.len() as u64;
        if chunk.offset + received > chunk.total {
            anyhow::bail!("这一片超出了声明的文件大小");
        }
        file.write_all(&piece)?;
    }
    Ok(received)
}

/// A scratch file beside the database, so the copy lands on the same filesystem
/// the database has room on. The random component keeps two concurrent calls
/// apart, since `VACUUM INTO` refuses an existing file.
fn scratch_path(app: &App, kind: &str) -> String {
    format!("{}.{kind}-{}.tmp", app.db.file(), &random_token()[..16])
}

/// The data page's figures.
///
/// Off the runtime, like the three routes below: `stats` counts every row of
/// `metric` and `ping_record` -- both WITHOUT ROWID, so each count is a full
/// index scan -- holding the connection the agents report through throughout. At
/// 2.2M rows that is 127 ms during which the public page and every agent report
/// also wait, growing with `retention_days`.
pub async fn db_stats(_: Admin, State(app): State<Shared>) -> Response {
    match tokio::task::spawn_blocking(move || app.db.stats()).await {
        Ok(Ok(stats)) => Json(stats).into_response(),
        Ok(Err(e)) => fail(e),
        Err(e) => fail(anyhow::anyhow!(e)),
    }
}

/// Returns a compact copy of the whole database.
///
/// The copy is written beside the live file and then unlinked while still open,
/// so it exists only for the duration of this response: a client that
/// disconnects partway through leaves nothing behind, and nothing on disk
/// outlives the download.
pub async fn db_backup(_: Admin, State(app): State<Shared>) -> Response {
    let path = scratch_path(&app, "backup");
    // Off the runtime: this reads the entire database while holding the
    // connection the agents write through.
    let copied = {
        let (app, path) = (app.clone(), path.clone());
        tokio::task::spawn_blocking(move || app.db.backup_into(&path)).await
    };
    if let Err(e) = copied.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        let _ = std::fs::remove_file(&path);
        return fail(e);
    }
    let opened = tokio::fs::File::open(&path).await;
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    let _ = std::fs::remove_file(&path);
    match opened {
        Ok(file) => (
            [
                (header::CONTENT_TYPE, "application/octet-stream".to_owned()),
                (header::CONTENT_LENGTH, size.to_string()),
                // The entire credential store: no shared cache may retain a copy.
                (header::CACHE_CONTROL, "no-store".to_owned()),
                (
                    header::CONTENT_DISPOSITION,
                    format!("attachment; filename=\"monitor-{}.db\"", Local::now().format("%Y%m%d-%H%M%S")),
                ),
            ],
            axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(file)),
        )
            .into_response(),
        Err(e) => fail(e),
    }
}

/// Replaces the live database with an uploaded backup, one chunk per request.
///
/// The upload streams to a file beside the database and is validated in full
/// before a single page is copied; see `Db::check_backup`. Afterwards every
/// session in the restored file is dropped and the caller is issued a new one: a
/// backup carries the session rows it held when taken, and restoring it must not
/// revive logged-out sessions.
pub async fn db_restore(
    _: Admin,
    State(app): State<Shared>,
    Query(chunk): Query<Chunk>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    // One fixed path, which is what allows the file's own length to constitute
    // the entire protocol.
    // ponytail: one upload in flight per hub. Two started simultaneously land on
    // this same name, and equal-sized chunks align their offsets, so they splice
    // rather than collide. The cost is a failed upload; distinguishing them would
    // require the upload id the protocol deliberately omits.
    let path = format!("{}.upload", app.db.file());
    let received = match receive(&path, &chunk, MAX_RESTORE, body).await {
        Ok(received) => received,
        Err(e) => return bad(&format!("{e:#}")),
    };
    if received < chunk.total {
        return Json(json!({"received": received})).into_response();
    }

    // Moved off the upload name before a byte is read. Splicing costs an upload;
    // what it must not cost is the live database, which without this it could:
    // the other upload would continue appending through its own handle while
    // `check_backup` reads the file and the page copy follows, and SQLite cannot
    // observe a write it did not make. A file that passed every gate would then
    // be copied over in a different state. Afterwards the other upload's next
    // chunk finds nothing and is told to restart, which is the error it already
    // has for an upload that disappeared.
    //
    // ponytail: the rename itself is not covered by a test. What it changes is
    // which path is open during the read, and reaching that would require a
    // second upload landing inside the copy. What is verified afterwards is that
    // neither name is left behind, in
    // `a_finished_restore_leaves_no_scratch_file_behind`.
    let source = scratch_path(&app, "restoring");
    if let Err(e) = std::fs::rename(&path, &source) {
        let _ = std::fs::remove_file(&path);
        return bad(&format!("上传收齐了却取不到文件：{e}"));
    }

    let outcome = restore(&app, &source).await;
    // SQLite writes a -wal and a -shm beside any file it opens in WAL mode, and a
    // plain copy of a running hub's database is exactly that. They are removed
    // when the connection closes cleanly; these three lines cover the case where
    // it does not.
    for leftover in [source.clone(), format!("{source}-wal"), format!("{source}-shm")] {
        let _ = std::fs::remove_file(leftover);
    }
    match outcome {
        Ok(()) => {
            // Agents authenticate at the handshake, and the tokens they hold may
            // now belong to different nodes, or to none. Dropping the senders ends
            // those loops; each reconnects against the restored database.
            app.agents.write().unwrap_or_else(|e| e.into_inner()).clear();
            invalidate_snapshot(&app);
            let cookie = match app.db.drop_all_sessions().and_then(|()| issue_session(&app, &headers)) {
                Ok(cookie) => cookie,
                Err(e) => return fail(e),
            };
            with_cookies(Json(json!({"ok": true})), [cookie])
        }
        Err(e) => bad(&format!("{e:#}")),
    }
}

async fn restore(app: &Shared, path: &str) -> Result<(), anyhow::Error> {
    // Both halves read the whole file, off the runtime: `PRAGMA integrity_check`
    // on a 256 MiB upload is not runtime work, and the copy that follows holds
    // the connection the agents write through.
    let (app, source) = (app.clone(), path.to_owned());
    tokio::task::spawn_blocking(move || {
        app.db.check_backup(&source)?;
        app.db.restore_from(&source)
    })
    .await?
}

/// Drops history beyond the retention window and rebuilds the file around what
/// remains, which is the only way SQLite returns the space to the filesystem.
pub async fn db_vacuum(_: Admin, State(app): State<Shared>) -> Response {
    let keep = app.db.retention_days();
    let app = app.clone();
    // A rebuild of the whole file, holding the connection the agents write
    // through, so it belongs on a blocking thread.
    let done = tokio::task::spawn_blocking(move || {
        let pruned = app.db.prune(keep)?;
        app.db.vacuum().map(|freed| json!({"pruned": pruned, "freed": freed}))
    })
    .await;
    match done.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        Ok(result) => Json(result).into_response(),
        Err(e) => fail(e),
    }
}

/// Installs an uploaded theme archive, one chunk per request.
///
/// The archive lands in the themes directory under a name `valid_short` rejects,
/// so a partial upload is invisible to both the theme list and the public page.
/// Installation is performed by `frontend::install`, which unpacks to a staging
/// directory and publishes with a rename: the switch is atomic, and the page is
/// never served from a partially written directory.
pub async fn upload_theme(
    _: Admin,
    State(app): State<Shared>,
    Query(chunk): Query<Chunk>,
    body: axum::body::Body,
) -> Response {
    let path = app.themes.join(".upload.tar.gz");
    let name = path.to_string_lossy().into_owned();
    let received = match receive(&name, &chunk, MAX_THEME, body).await {
        Ok(received) => received,
        Err(e) => return bad(&format!("{e:#}")),
    };
    if received < chunk.total {
        return Json(json!({"received": received})).into_response();
    }

    // Moved off the shared upload name for the same reason as the restore path: a
    // second upload landing on it could continue writing while this archive is
    // read, leaving the unpacker reading a file changed beneath it. Named so
    // `valid_short` still rejects it, keeping a partial archive out of the theme
    // list.
    let source = app.themes.join(format!(".installing-{}.tar.gz", &random_token()[..16]));
    if let Err(e) = std::fs::rename(&path, &source) {
        let _ = std::fs::remove_file(&path);
        return bad(&format!("上传收齐了却取不到文件：{e}"));
    }

    // Off the runtime: gunzip plus a few thousand small writes.
    let installed = {
        let (app, path) = (app.clone(), source.clone());
        tokio::task::spawn_blocking(move || {
            crate::frontend::install(&app.themes, std::fs::File::open(&path)?, None)
        })
        .await
    };
    let _ = std::fs::remove_file(&source);
    match installed.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        Ok(theme) => Json(json!({"theme": theme})).into_response(),
        Err(e) => bad(&format!("{e:#}")),
    }
}

/// The asset a theme repository publishes, and the only name the hub fetches: the
/// same `theme.tar.gz` the upload button accepts.
const ARCHIVE: &str = "theme.tar.gz";

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    #[serde(default)]
    assets: Vec<Asset>,
}

#[derive(Deserialize)]
struct Asset {
    name: String,
}

/// The `<owner>/<repo>` a theme's `url` names, where it names a GitHub repository
/// at all.
///
/// An allowlist rather than a filter. Every address the update path fetches is
/// constructed from these two strings, so nothing in a manifest can direct the
/// hub at a host it did not choose, which is why no private-address check is
/// needed here. The only host that is not github.com is the GitHub proxy in the
/// panel's settings, configured by the operator and already used by the agent
/// relay.
fn github_repo(url: &str) -> Option<(&str, &str)> {
    let (owner, rest) = url.strip_prefix("https://github.com/")?.split_once('/')?;
    // A link to a branch or a file is still a link to the repository.
    let repo = rest.split('/').next()?;
    let repo = repo.strip_suffix(".git").unwrap_or(repo);
    (path_segment(owner) && path_segment(repo)).then_some((owner, repo))
}

/// One URL path segment the hub will build a github.com address from: nothing
/// that opens a new segment, and nothing that escapes the current one.
fn path_segment(segment: &str) -> bool {
    !segment.is_empty()
        && segment != "."
        && segment != ".."
        && segment.bytes().all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
}

/// Reinstalls one theme from the latest GitHub release of the repository its
/// manifest names.
///
/// The manifest supplies `<owner>/<repo>` and nothing more: the release is read
/// from api.github.com and the archive from github.com, both at addresses the hub
/// constructs itself, so no URL from the theme is ever followed. The installed
/// version is compared against the release tag first, which is all most
/// invocations do, making this also the check-for-updates action.
pub async fn update_theme(_: Admin, State(app): State<Shared>, Path(short): Path<String>) -> Response {
    match update(&app, &short).await {
        Ok((updated, version)) => Json(json!({"updated": updated, "version": version})).into_response(),
        Err(e) => bad(&format!("{e:#}")),
    }
}

async fn update(app: &App, short: &str) -> Result<(bool, String), anyhow::Error> {
    use anyhow::{bail, Context};

    let installed = crate::frontend::themes(app)?
        .into_iter()
        .find(|theme| theme.short == short)
        .context("没有这个主题")?;
    let (owner, repo) = github_repo(&installed.url)
        .context("这个主题的 url 不是 https://github.com/<owner>/<repo>，只能手动上传新包")?;

    // Unauthenticated: 60 requests per hour from this address, ample for a manual
    // action. GitHub returns 403 without a User-Agent.
    let release: Release = app
        .http
        .get(format!("https://api.github.com/repos/{owner}/{repo}/releases/latest"))
        .header(header::USER_AGENT, "monitor-hub")
        .send()
        .await?
        .error_for_status()
        .with_context(|| format!("读不到 {owner}/{repo} 的最新 release"))?
        .json()
        .await?;

    // Tags read `v1.2.3` while manifests carry `1.2.3`. Equal means up to date;
    // anything else is installed, including a deliberate downgrade, since the
    // release is what the author published.
    let tag = &release.tag_name;
    if tag.strip_prefix('v').unwrap_or(tag) == installed.version {
        return Ok((false, installed.version));
    }
    if !path_segment(tag) {
        bail!("release 的 tag {tag:?} 不能出现在下载地址里");
    }
    // Checked here rather than by downloading and reading a 404: the asset name is
    // the contract, and stating so is the entire error message.
    if !release.assets.iter().any(|asset| asset.name == ARCHIVE) {
        bail!("release {tag} 里没有 {ARCHIVE}");
    }

    // Through the panel's GitHub proxy when one is configured, the archive being
    // the part a blocked network cannot reach. The API call above is not proxied:
    // most proxies front only releases, and a hub that cannot read the tag still
    // has the upload path.
    let url =
        crate::proxied(app, format!("https://github.com/{owner}/{repo}/releases/download/{tag}/{ARCHIVE}"));
    let response =
        app.http.get(url).timeout(std::time::Duration::from_secs(120)).send().await?.error_for_status()?;
    // The transfer stops at Content-Length, so checking it checks the body: a
    // header understating the archive cannot make more arrive. GitHub always
    // sends one; a proxy that omits it is refused rather than read unbounded.
    match response.content_length() {
        Some(size) if size <= MAX_THEME => {}
        Some(size) => bail!("主题包 {} MiB，超过 {} MiB 的上限", size / 1024 / 1024, MAX_THEME / 1024 / 1024),
        None => bail!("下载没有给出大小，无法确认它在 {} MiB 以内", MAX_THEME / 1024 / 1024),
    }
    let archive = response.bytes().await?;

    // The same unpacking, validation and atomic replace an upload undergoes,
    // constrained to the theme it may replace. The built-in theme has no
    // directory until this runs: updating it writes one, which then serves in
    // place of the embedded copy until it is deleted.
    let (themes, short) = (app.themes.clone(), short.to_owned());
    let theme = tokio::task::spawn_blocking(move || {
        crate::frontend::install(&themes, std::io::Cursor::new(archive), Some(&short))
    })
    .await??;
    Ok((true, theme.version))
}

/// The thumbnail the theme list displays, where the theme provides one. A theme
/// without one returns 404, on which the panel hides the image, so nothing need
/// report whether a preview exists.
pub async fn theme_preview(_: Admin, State(app): State<Shared>, Path(short): Path<String>) -> Response {
    match crate::frontend::preview(&app.themes, &short) {
        // Not cached: reinstalling a theme under the same name also replaces the
        // image, and this is a panel-only request for a local file.
        Some(png) => {
            ([(header::CONTENT_TYPE, "image/png"), (header::CACHE_CONTROL, "no-cache")], png).into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

/// Deletes an installed theme. Deleting the one in use is permitted: the public
/// page falls back to the built-in theme from the next request, the same path a
/// broken theme already takes, and leaving the setting intact means reinstalling
/// the theme restores it.
pub async fn delete_theme(_: Admin, State(app): State<Shared>, Path(short): Path<String>) -> Response {
    match crate::frontend::remove(&app.themes, &short) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => bad(&format!("{e:#}")),
    }
}

pub async fn themes(_: Admin, State(app): State<Shared>) -> Response {
    match crate::frontend::themes(&app) {
        Ok(themes) => Json(json!({"themes": themes})).into_response(),
        Err(e) => fail(e),
    }
}

// ---- plugins(U5):上传、启停删、测试、日志与 kv ----

/// 插件上传包(tar.gz)的累计字节上限(R11)。独立于主题的 32 MiB:一个插件是
/// 一份 manifest 加一个 wasm 模块,8 MiB 已是宽裕。这条路由因此挂在
/// [`MAX_CHUNK`] 的 merge 子 router 上(见 main),reverse proxy 需要放行的
/// 单请求大小与备份分片相同。
pub const MAX_PLUGIN: u64 = 8 * 1024 * 1024;

/// 解包防护,与 frontend 的主题包防护同一套思路,只是全程内存、不落盘:
/// 条目数、单个 entry 与解压后的总量分别封顶,把三种形态的解压炸弹都挡在
/// 写库之前。上限比主题宽(200 条 / 16 MiB / 32 MiB),因为插件包预期就是
/// 两个文件,任何接近上限的包都可疑。
const PLUGIN_MAX_ENTRIES: usize = 200;
const PLUGIN_MAX_FILE: u64 = 16 << 20;
const PLUGIN_MAX_EXPANDED: u64 = 32 << 20;
/// manifest 与 wasm 模块各自的体量上限:manifest 是几十行 TOML,wasm 模块
/// 在 16 MiB 封顶处与单 entry 上限重合。
const PLUGIN_MANIFEST_MAX: usize = 64 * 1024;
const PLUGIN_WASM_MAX: usize = 16 << 20;

/// 面板 kv 的 value 上限,与插件的 host_kv_set 同限:面板能写的不能比插件
/// 运行时能写的多,否则面板成了绕过插件存储限额的后门。
const PLUGIN_KV_VALUE_MAX: usize = 8 * 1024;
/// 面板 kv 的 key 上限,给 `plugin.<id>:<key>` 的命名空间留出余量。
const PLUGIN_KV_KEY_MAX: usize = 128;

/// Installs an uploaded plugin package (R11): a `multipart/form-data` request
/// whose `plugin` field carries the plugin's `tar.gz`.
///
/// 收字节(上限 [`MAX_PLUGIN`])在异步侧完成;解包、校验、编译与写库整体
/// 挪进 spawn_blocking——wasm 编译是百毫秒级的 CPU 工作,不该占着调度线程。
/// 一个包要么整体验证通过,要么什么都不写:写库发生在解包、manifest 校验
/// 与重复检查全部通过之后,而编译(预热校验)失败也照常入库——作者需要
/// 在面板上看到原因,而不是被迫从日志里找(KTD10:上传后默认不启用)。
pub async fn upload_plugin(_: Admin, State(app): State<Shared>, mut multipart: Multipart) -> Response {
    // 找名为 plugin 的文件字段,边收边计数:上限检查不等包收完,多出的第一
    // 个字节就被拒绝,不用把 8 MiB 都吃进内存再丢弃。
    let mut bytes: Vec<u8> = Vec::new();
    let mut archive: Option<Vec<u8>> = None;
    while let Some(field) = match multipart.next_field().await {
        Ok(field) => field,
        Err(e) => return multipart_failed(e),
    } {
        if field.name() != Some("plugin") {
            continue; // 别的字段(比如未来的注释)收下即丢,不报错。
        }
        let mut field = field;
        loop {
            match field.chunk().await {
                Ok(Some(piece)) => {
                    bytes.extend_from_slice(&piece);
                    if bytes.len() as u64 > MAX_PLUGIN {
                        return bad(&format!("插件包超过 {} MiB 的上限", MAX_PLUGIN / 1024 / 1024));
                    }
                }
                Ok(None) => break,
                Err(e) => return multipart_failed(e),
            }
        }
        archive = Some(std::mem::take(&mut bytes));
        break; // 第一个 plugin 字段为准,重复出现的同名字段忽略。
    }
    let Some(archive) = archive else {
        return bad("缺少名为 plugin 的文件字段");
    };

    let installed = {
        let app = app.clone();
        tokio::task::spawn_blocking(move || install_plugin(&app, &archive))
    }
    .await;
    match installed.map_err(|e| anyhow::anyhow!(e)).and_then(|r| r) {
        Ok(body) => Json(body).into_response(),
        Err(e) => bad(&format!("{e:#}")),
    }
}

/// multipart 中断时的响应。超限有两条路:Multipart 提取器自己的 body limit
/// (main 在上传路由上配成 [`MAX_CHUNK`],与 tower 的层同值)先断流,或这里的
/// 累计上限后到——都归成同一句 400,调用侧看到的说法只有一种。
fn multipart_failed(e: MultipartError) -> Response {
    if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
        bad(&format!("插件包超过 {} MiB 的上限", MAX_PLUGIN / 1024 / 1024))
    } else {
        bad(&format!("上传的 multipart 请求解析失败：{e}"))
    }
}

/// 解包、校验并写库,`upload_plugin` 的同步主体。每一步失败都带着可操作的
/// 原因返回(它就是 400 的响应体)。
fn install_plugin(app: &App, archive: &[u8]) -> Result<Value, anyhow::Error> {
    use anyhow::{bail, Context};

    let files = unpack_plugin(archive)?;
    let toml_text = files.get("plugin.toml").context("插件包里没有 plugin.toml")?;
    if toml_text.len() > PLUGIN_MANIFEST_MAX {
        bail!("plugin.toml 超过 64 KiB");
    }
    let toml_text = String::from_utf8(toml_text.clone()).context("plugin.toml 不是合法的 UTF-8 文本")?;
    let manifest = Manifest::parse(&toml_text)?;
    let wasm =
        files.get(&manifest.wasm_entry).with_context(|| format!("插件包里没有 {}", manifest.wasm_entry))?;
    if wasm.len() > PLUGIN_WASM_MAX {
        bail!("{} 超过 {} MiB 的上限", manifest.wasm_entry, PLUGIN_WASM_MAX >> 20);
    }
    let wasm_sha256 = hex::encode(Sha256::digest(wasm));

    // 重复的 plugin_id 是覆盖不是升级:manifest_json 与 wasm_blob 都整行替换
    // 会丢掉 enabled 与状态,所以先拒绝,让作者显式删掉旧版再上传。
    if app.db.list_plugins()?.iter().any(|r| r.plugin_id == manifest.plugin_id) {
        bail!("插件 {} 已存在；先删除旧版本再上传", manifest.plugin_id);
    }

    // 预热校验:用一条临时行(id=0,不落库)走真实的加载路径,把「manifest 写
    // 错了」「模块缺导出」「模块编译不过」在上传时就暴露。失败不拒绝入库:
    // status 保持 disabled、原因写进 last_error,面板上点开就能看到。
    let candidate = PluginRow {
        id: 0,
        plugin_id: manifest.plugin_id.clone(),
        name: manifest.name.clone(),
        version: manifest.version.clone(),
        manifest_json: toml_text.clone(),
        wasm_blob: wasm.clone(),
        wasm_sha256: String::new(),
        enabled: false,
        status: "disabled".into(),
        last_error: None,
        uploaded_at: 0,
    };
    let last_error = plugin::load(&app.engine, &candidate).err().map(|e| format!("{e:#}"));

    let row = app.db.create_plugin(
        &manifest.plugin_id,
        &manifest.name,
        &manifest.version,
        &toml_text,
        wasm,
        &wasm_sha256,
    )?;
    if let Some(error) = &last_error {
        app.db.set_plugin_status(row.id, "disabled", Some(error))?;
    }
    Ok(json!({
        "id": row.id,
        "plugin_id": row.plugin_id,
        "status": "disabled",
        "last_error": last_error,
    }))
}

/// 把 tar.gz 的内容解进一个 `文件名 -> 字节` 的表。不落盘:上限之内整个包
/// 都在内存里,而 8 MiB 的入站上限已经把这里能见到的东西封住了。文件名取
/// 路径的最后一段,`./plugin.toml` 与带一层目录的包都能取到。
fn unpack_plugin(archive: &[u8]) -> Result<HashMap<String, Vec<u8>>, anyhow::Error> {
    use anyhow::{bail, Context};
    use std::io::Read;
    use std::path::{Component, Path};

    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    let mut files = HashMap::new();
    let mut expanded = 0u64;
    for (seen, entry) in tar.entries()?.enumerate() {
        let mut entry = entry.context("插件包不是有效的 tar.gz")?;
        if seen >= PLUGIN_MAX_ENTRIES {
            bail!("插件包里的条目超过 {} 个", PLUGIN_MAX_ENTRIES);
        }
        // 路径先于内容:绝对路径与 `..` 在这里就该被拒,后面的上限检查才有
        // 一个可信的路径可报。`./` 前缀是 tar 的常态,不算越界。
        let path = entry.path()?.to_path_buf();
        for component in Path::new(&path).components() {
            match component {
                Component::ParentDir => bail!("插件包里的路径包含 `..`：{}", path.display()),
                Component::RootDir | Component::Prefix(_) => {
                    bail!("插件包里的路径是绝对路径：{}", path.display())
                }
                _ => {}
            }
        }
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            continue; // 目录条目不携带数据,跳过而不是拒绝:打包工具常带它们。
        }
        if !kind.is_file() {
            bail!("插件包里有不支持的条目（仅接受普通文件）：{}", path.display());
        }
        let size = entry.size();
        if size > PLUGIN_MAX_FILE {
            bail!("{} 超过单个文件 {} MiB 的上限", path.display(), PLUGIN_MAX_FILE >> 20);
        }
        // 减法而不是加法:两个上限值相加会溢出,而 size 已知是较小的一方。
        if expanded > PLUGIN_MAX_EXPANDED - size {
            bail!("插件包解压后超过 {} MiB", PLUGIN_MAX_EXPANDED >> 20);
        }
        expanded += size;
        let mut data = Vec::with_capacity(size as usize);
        entry.read_to_end(&mut data).with_context(|| format!("读取 {} 失败", path.display()))?;
        let name =
            path.file_name().and_then(|n| n.to_str()).context("插件包里的文件名不是合法的 UTF-8")?.to_owned();
        files.insert(name, data);
    }
    if files.is_empty() {
        bail!("插件包里没有任何文件");
    }
    Ok(files)
}

/// 面板的插件列表(R12)。manifest_json 就在行里,把 subscribes 解出来一起
/// 返回,前端画事件徽标不必再猜。
pub async fn list_plugins(_: Admin, State(app): State<Shared>) -> Response {
    match app.db.plugin_summaries() {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|r| {
                    // manifest 上传时已通过校验;这里容错而不是失败,一行坏
                    // manifest(手工改库)不该让整个列表 500。
                    let subscribes =
                        Manifest::parse(&r.manifest_json).map(|m| m.subscribes).unwrap_or_default();
                    json!({
                        "id": r.id,
                        "plugin_id": r.plugin_id,
                        "name": r.name,
                        "version": r.version,
                        "enabled": r.enabled,
                        "status": r.status,
                        "last_error": r.last_error,
                        "uploaded_at": r.uploaded_at,
                        "subscribes": subscribes,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => fail(e),
    }
}

/// 删除插件,连同它的 kv 行。db 行、setting 行、内存里的已加载实例三处一起
/// 收:任何一处留下都会以别的方式回来——行留着列表里就还有它,kv 留着删除
/// 再重传同名插件会捡到旧的渠道配置,内存留着它还会继续收事件。
pub async fn delete_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let row = match app.db.get_plugin(id) {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    };
    let gone = app.db.delete_plugin(id).and_then(|()| app.db.delete_plugin_kv(&row.plugin_id));
    match gone {
        // db 行与 kv 都删净之后才动内存:失败路径上插件保持原状,重试即是。
        // 返回值是删掉的 kv 行数,0 也是成功。
        Ok(_) => {
            app.plugins.write().unwrap_or_else(|e| e.into_inner()).remove_plugin(id);
            StatusCode::NO_CONTENT.into_response()
        }
        // get_plugin 与 delete_plugin 之间的窗口里行没了:也是 404。
        Err(e) if e.to_string().contains("no plugin") => StatusCode::NOT_FOUND.into_response(),
        Err(e) => fail(e),
    }
}

/// 启用插件:先写 db 的开关,再装进内存。加载失败时 Registry 已把 failed 与
/// 原因落库,这里转成 400 带给面板——作者改完包重新上传即可。
pub async fn enable_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    match app.db.get_plugin(id) {
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    }
    if let Err(e) = app.db.set_plugin_enabled(id, true) {
        return fail(e);
    }
    match app.plugins.write().unwrap_or_else(|e| e.into_inner()).enable_plugin(&app, id) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => bad(&format!("插件加载失败，已标记为 failed：{e:#}")),
    }
}

/// 禁用插件:与启用对称,只是加载不可能失败,没有错误分支可言。
pub async fn disable_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    match app.db.get_plugin(id) {
        Ok(Some(_)) => {}
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    }
    if let Err(e) = app.db.set_plugin_enabled(id, false) {
        return fail(e);
    }
    app.plugins.write().unwrap_or_else(|e| e.into_inner()).disable_plugin(id);
    Json(json!({"ok": true})).into_response()
}

/// 测试通知(R12):合成一个明天的 ExpirySoon 事件,走与真实派发完全相同的
/// 执行路径(超时、fuel、宿主函数),但绕过 emit 与 notification_log——一次
/// 手工测试不占幂等键,真实事件的成功与否不该被它覆盖(U4 的 dispatch_one)。
pub async fn test_plugin(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let row = match app.db.get_plugin(id) {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    };
    let event = Event::ExpirySoon {
        node_id: 0,
        name: "test".into(),
        expires_at: (Utc::now() + chrono::Duration::days(1)).format("%Y-%m-%d").to_string(),
        days_left: 1,
        threshold_days: 1,
    };
    // 读锁的 guard 不是 Send,不能横跨 handler 的 await(handler 的 future 必须是
    // Send);而 dispatch_one 又只在 &self 上工作。把它整个挪进 blocking 线程,
    // 用预先取好的 runtime handle 驱动——guard 只活在那条同步闭包里,内部
    // run_one 的 spawn 与超时照常落在 runtime 上。
    let outcome = {
        let app = app.clone();
        let handle = tokio::runtime::Handle::current();
        tokio::task::spawn_blocking(move || {
            let registry = app.plugins.read().unwrap_or_else(|e| e.into_inner());
            handle.block_on(registry.dispatch_one(id, &event))
        })
        .await
    };
    match outcome {
        Ok(Ok(entry)) => Json(json!({
            "plugin_id": row.plugin_id,
            "wasm_result": entry.result,
            "elapsed_ms": entry.elapsed_ms,
        }))
        .into_response(),
        // 未加载(未启用或加载失败)是调用侧可修复的状态,400 而不是 500。
        Ok(Err(_)) => bad("插件未启用或加载失败；先启用它再测试"),
        Err(e) => fail(anyhow::anyhow!(e)),
    }
}

/// 一个插件的派发日志(R16):内存环形缓冲的快照按 plugin_id 过滤,取最近
/// 100 条。缓冲是进程内的,重启后为空——面板把它当「刚才发生了什么」看,
/// 长期审计在 notification_log。
pub async fn plugin_dispatch_log(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let row = match app.db.get_plugin(id) {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    };
    let entries: Vec<_> = app
        .plugins
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .dispatch_log_snapshot()
        .into_iter()
        .filter(|entry| entry.plugin_id == row.plugin_id)
        .take(100)
        .collect();
    Json(entries).into_response()
}

/// 写一个插件的 kv 行(R13):渠道配置这类「面板替插件填」的值。落在与
/// host_kv_set 相同的 `plugin.<plugin_id>:<key>` 命名空间与相同的 8 KiB 上限
/// 里,插件读到的与作者填的是同一行。
pub async fn set_plugin_kv(
    _: Admin,
    State(app): State<Shared>,
    Path((id, key)): Path<(i64, String)>,
    Json(body): Json<Value>,
) -> Response {
    let row = match app.db.get_plugin(id) {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    };
    if key.trim().is_empty() {
        return bad("key 不能为空");
    }
    if key.len() > PLUGIN_KV_KEY_MAX {
        return bad(&format!("key 超过 {PLUGIN_KV_KEY_MAX} 字节的上限"));
    }
    if key.contains(':') {
        return bad("key 不能包含 ':'（它是 kv 命名空间的分隔符）");
    }
    let Some(value) = body.get("value").and_then(Value::as_str) else {
        return bad("body 必须是 {\"value\": \"...\"} 形式的对象");
    };
    if value.len() > PLUGIN_KV_VALUE_MAX {
        return bad(&format!(
            "value 超过 {} KiB 的上限（与插件的 host_kv_set 同限）",
            PLUGIN_KV_VALUE_MAX / 1024
        ));
    }
    match app.db.set(&format!("plugin.{}:{key}", row.plugin_id), value) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => fail(e),
    }
}

/// 列出一个插件的全部 kv 行(R13),key 去掉命名空间前缀,面板照原样回填表单。
pub async fn list_plugin_kv(_: Admin, State(app): State<Shared>, Path(id): Path<i64>) -> Response {
    let row = match app.db.get_plugin(id) {
        Ok(Some(row)) => row,
        Ok(None) => return StatusCode::NOT_FOUND.into_response(),
        Err(e) => return fail(e),
    };
    match app.db.plugin_kv(&row.plugin_id) {
        Ok(pairs) => Json(
            pairs.into_iter().map(|(key, value)| json!({"key": key, "value": value})).collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => fail(e),
    }
}

/// Every live session, with the caller's own marked.
///
/// `id` is the stored SHA-256 of the session token rather than the token itself:
/// it identifies a row without being presentable as a cookie.
pub async fn sessions(_: Admin, State(app): State<Shared>, headers: HeaderMap) -> Response {
    let mine = current_session(&headers);
    match app.db.sessions() {
        Ok(rows) => Json(
            rows.into_iter()
                .map(|(hash, expires_at)| {
                    json!({
                        "current": mine.as_deref() == Some(hash.as_str()),
                        "created_at": issued_at(expires_at),
                        "id": hash,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(e) => fail(e),
    }
}

/// Deleting a row that no longer exists is not an error: two panels open on the
/// same list both achieve the requested sign-out.
pub async fn delete_session(_: Admin, State(app): State<Shared>, Path(id): Path<String>) -> Response {
    match app.db.drop_session(&id) {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => fail(e),
    }
}

pub async fn settings(_: Admin, State(app): State<Shared>) -> Json<Value> {
    let mut out = serde_json::Map::new();
    for key in READABLE_SETTINGS {
        out.insert((*key).to_owned(), json!(app.db.get(key).unwrap_or_default()));
    }
    // The one readable key with a default that also rejects the empty string:
    // `setting_error` below refuses "" and `save_settings` writes nothing when any
    // key fails, so a hub where this was never set returned "" here and then
    // rejected the entire settings form, naming a field that was never edited.
    // `retention_days()` already holds the default `prune` and the data page read,
    // so it answers here as well.
    out.insert("retention_days".into(), json!(app.db.retention_days().to_string()));
    out.insert(
        "github_secret_set".into(),
        json!(app.db.get("github_client_secret").is_some_and(|v| !v.is_empty())),
    );
    // Read-only here. A window is opened and closed through its own route, so the
    // key is always one the hub generated, and `save_settings` continues to refuse
    // both names.
    for key in ["register_key", "register_until"] {
        out.insert(key.into(), json!(app.db.get(key).unwrap_or_default()));
    }
    Json(Value::Object(out))
}

/// Why one setting cannot be stored, or `None` when it can.
///
/// Separate from the write below because every key is validated before any is
/// written: changing the password drops every session, and a 400 raised
/// afterwards -- on a later key, in whatever order the map iterates -- carries no
/// Set-Cookie, signing the admin out of every device through a password change
/// the UI reported as rejected.
fn setting_error(app: &App, key: &str, value: &Value) -> Option<String> {
    // Settings are stored as text. A caller sending the natural JSON type --
    // `{"public_page": false}`, `{"retention_days": 7}` -- was formerly skipped by
    // a bare `continue`, so nothing was written while the response reported
    // success.
    let Some(value) = value.as_str() else { return Some(format!("{key} must be a string")) };
    match key {
        "theme" if !crate::frontend::selectable(app, value) => Some("theme is not installed".into()),
        // Housekeeping clamps whatever it reads, so an unparsable value would be
        // stored, echoed back, and silently mean 7 days indefinitely.
        "retention_days" if !value.parse::<i64>().is_ok_and(|d| (1..=3_650).contains(&d)) => {
            Some("retention days must be a number from 1 to 3650".into())
        }
        // The hub fetches this URL itself, so it must be one: a scheme it cannot
        // speak turns every agent download into a 502 that says nothing about the
        // setting responsible.
        //
        // https only. What returns from this host is the agent binary, which
        // `install.sh` writes to /opt/monitor and starts on every node provisioned
        // here; over http:// anyone on the path between the hub and the mirror
        // chooses that binary, while the node still sees a valid TLS connection to
        // the hub.
        "github_proxy" if !(value.is_empty() || value.starts_with("https://")) => {
            Some("GitHub proxy must start with https://: the agent binary is fetched through it and installed on every node".into())
        }
        "admin_password" if value.len() < 12 => Some("password must be at least 12 characters".into()),
        "admin_password" => None,
        k if READABLE_SETTINGS.contains(&k) || k == "github_client_secret" => None,
        _ => Some(format!("unknown setting: {key}")),
    }
}

pub async fn save_settings(
    _: Admin,
    State(app): State<Shared>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    let Some(map) = body.as_object() else { return bad("expected an object") };
    for (key, value) in map {
        if let Some(message) = setting_error(&app, key, value) {
            return bad(&message);
        }
    }
    // Set when the password changed, so the caller receives a fresh session rather
    // than being logged out by their own change.
    let mut reissued = String::new();
    for (key, value) in map {
        let value = value.as_str().unwrap_or_default();
        // Changing the password logs out every existing session; the caller
        // receives a replacement.
        if key == "admin_password" {
            match hash_password(value).and_then(|h| {
                app.db.set("admin_password_hash", &h)?;
                app.db.drop_all_sessions()?;
                issue_session(&app, &headers)
            }) {
                Ok(cookie) => reissued = cookie,
                Err(e) => return fail(e),
            }
            continue;
        }
        if let Err(e) = app.db.set(key, value) {
            return fail(e);
        }
    }
    with_cookies(Json(json!({"ok": true})), [reissued])
}

#[cfg(test)]
mod tests {
    use super::*;
    // Sessions remain hashed; only node tokens are stored in the clear.
    use crate::auth::sha256;
    use crate::db::Db;

    fn domain_headers() -> HeaderMap {
        HeaderMap::from_iter([
            (header::HOST, "monitor.example.com".parse().unwrap()),
            (header::HeaderName::from_static("x-forwarded-proto"), "https".parse().unwrap()),
        ])
    }

    fn app() -> App {
        App::for_test(Db::open(":memory:").unwrap())
    }

    #[tokio::test]
    async fn provisioning_requires_the_current_https_domain_entry() {
        let no_site = app();
        let mut state = app();
        state.site = "https://monitor.example.com".into();
        let app = std::sync::Arc::new(state);
        let good = domain_headers();
        assert!(provisioning_allowed(&app, &good));
        let mut plain = good.clone();
        plain.insert("x-forwarded-proto", "http".parse().unwrap());
        assert!(
            !provisioning_allowed(&app, &plain),
            "--site cannot override an explicitly plaintext request"
        );
        for host in ["127.0.0.1:9911", "[::1]:9911", "198.51.100.1", "2130706433", "localhost"] {
            let mut headers = good.clone();
            headers.insert(header::HOST, host.parse().unwrap());
            let node = serde_json::from_value(json!({"name":"blocked"})).unwrap();
            assert_eq!(
                create_node(Admin, State(app.clone()), headers.clone(), Ok(Json(node))).await.status(),
                StatusCode::FORBIDDEN
            );
            assert_eq!(
                open_register(Admin, State(app.clone()), headers.clone()).await.status(),
                StatusCode::FORBIDDEN
            );
            assert_eq!(
                agent_register(
                    State(app.clone()),
                    ConnectInfo("127.0.0.1:1".parse().unwrap()),
                    headers,
                    "blocked".into()
                )
                .await
                .status(),
                StatusCode::FORBIDDEN
            );
        }
        let mut headers = good.clone();
        headers.insert(header::ORIGIN, "http://127.0.0.1:9911".parse().unwrap());
        assert!(!provisioning_allowed(&app, &headers));
        assert!(app.db.nodes().unwrap().is_empty());
        assert!(app.db.get("register_key").is_none());
        headers = good;
        headers.remove("x-forwarded-proto");
        assert!(!provisioning_allowed(&no_site, &headers));
        for site in [
            "http://monitor.example.com",
            "https://198.51.100.1",
            "https://user@monitor.example.com",
            "https://monitor.example.com/path",
        ] {
            assert!(https_domain(site).is_none());
        }
    }

    /// Whatever this accepts is pushed to every assigned agent and passed directly
    /// to `lookup_host`. Forms it cannot resolve return -1 indefinitely, which the
    /// chart draws as a probe losing every packet, so the check must match what
    /// the error message claims.
    #[test]
    fn a_probe_target_must_be_something_the_agent_can_resolve() {
        // Each of these causes `lookup_host` to return an error, verified against
        // it: a bare IPv6 address is all colons, and the other two omit the half
        // the message requires.
        for bad in
            ["2606:4700:4700::1111", ":443", "example.com:", "1.1.1.1", "1.1.1.1:0", "[::1]:x", "[::1]"]
        {
            assert!(!valid_target(bad), "{bad}");
        }
        for good in ["1.1.1.1:443", "[2606:4700:4700::1111]:443", "example.com:80", "[::1]:1"] {
            assert!(valid_target(good), "{good}");
        }
    }

    /// The check above is meaningful only if it runs on the stored string: what
    /// reaches the agent is the stored value, and `lookup_host` rejects
    /// `"1.1.1.1:443 "` outright -- the permanent -1 `valid_target` exists to
    /// prevent, reachable through a check that passed.
    #[tokio::test]
    async fn a_probe_target_is_stored_as_the_string_that_was_checked() {
        let app = std::sync::Arc::new(app());
        let save = |name: &str, target: &str| {
            let task = PingTask {
                id: 0,
                name: name.to_owned(),
                target: target.to_owned(),
                interval: 60,
                nodes: vec![],
            };
            save_ping_task(Admin, State(app.clone()), Json(task))
        };
        assert_eq!(save(" 探测 ", "1.1.1.1:443 ").await.status(), StatusCode::OK);
        let stored = &app.db.ping_tasks().unwrap()[0];
        assert_eq!(stored.target, "1.1.1.1:443", "the agent gets this string, not the one that was checked");
        assert_eq!(stored.name, "探测", "and it labels an anonymous chart");
        // Trimming must not turn a blank entry into a saved row.
        assert_eq!(save("   ", "   ").await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(app.db.ping_tasks().unwrap().len(), 1);
    }

    /// The same rule as `retention_days`, applied to the other value this hub
    /// clamps downstream: an out-of-range value must fail, or it silently becomes
    /// a different one. Below the floor that value is 5 seconds, the fastest probe
    /// available, and the panel reaches 0 simply by clearing its interval field,
    /// since `Number("")` is 0.
    #[tokio::test]
    async fn a_probe_interval_out_of_range_is_refused_rather_than_clamped() {
        let app = std::sync::Arc::new(app());
        let save = |interval| {
            let task = PingTask {
                id: 0,
                name: "probe".into(),
                target: "1.1.1.1:443".into(),
                interval,
                nodes: vec![],
            };
            save_ping_task(Admin, State(app.clone()), Json(task))
        };
        for refused in [0, -1, 4, 3_601, i64::MAX] {
            assert_eq!(save(refused).await.status(), StatusCode::BAD_REQUEST, "{refused}");
        }
        assert!(app.db.ping_tasks().unwrap().is_empty(), "a refused interval must not store a probe");

        // Both ends of the range still save, storing exactly what was sent.
        for ok in [5, 60, 3_600] {
            assert_eq!(save(ok).await.status(), StatusCode::OK, "{ok}");
        }
        let stored: Vec<i64> = app.db.ping_tasks().unwrap().iter().map(|t| t.interval).collect();
        assert_eq!(stored, vec![5, 60, 3_600]);
    }

    /// The update path follows a manifest's `url` to build a download address, so
    /// what counts as a GitHub repository constitutes the entire trust boundary:
    /// whatever this accepts, the hub will fetch.
    #[test]
    fn only_a_github_repository_url_can_name_a_release_to_download() {
        assert_eq!(
            github_repo("https://github.com/monitor-probe/monitor"),
            Some(("monitor-probe", "monitor"))
        );
        // A link to the repository, in whatever form the author wrote it.
        assert_eq!(github_repo("https://github.com/a/b.git"), Some(("a", "b")));
        assert_eq!(github_repo("https://github.com/a/b/tree/main"), Some(("a", "b")));
        assert_eq!(github_repo("https://github.com/a/b/"), Some(("a", "b")));

        for hostile in [
            "",
            // Not github.com, however much of it appears in the string.
            "http://github.com/a/b",
            "https://github.com.evil.test/a/b",
            "https://github.com@evil.test/a/b",
            "https://evil.test/https://github.com/a/b",
            // On github.com, but naming no repository to fetch from.
            "https://github.com/a",
            "https://github.com//b",
            "https://github.com/../../etc/passwd",
            "https://github.com/a/..",
            // Anything that could open a segment of its own in the URL built from
            // it, whether encoded, queried or fragmented.
            "https://github.com/a/b%2f..%2fc",
            "https://github.com/a/b?x=1",
            "https://github.com/a b",
        ] {
            assert_eq!(github_repo(hostile), None, "{hostile} must not name a download");
        }

        // The release tag also lands in that URL, arriving from the API rather
        // than the manifest.
        assert!(path_segment("v0.1.15") && path_segment("2024.1"));
        assert!(!path_segment("release/1.0") && !path_segment("..") && !path_segment(""));
    }

    /// The entire chunked-upload protocol: an upload is only ever as long as what
    /// has landed, so a piece continues it, restarts it, or is refused.
    #[tokio::test]
    async fn a_chunk_continues_an_upload_only_where_the_last_one_ended() {
        let path = std::env::temp_dir().join(format!("monitor-chunk-{}", std::process::id()));
        let path = path.to_str().unwrap();
        let piece = |offset, total| Chunk { offset, total };
        let body = |bytes: &'static [u8]| axum::body::Body::from(bytes);

        // Two pieces in order, with the length indicating where the next begins.
        assert_eq!(receive(path, &piece(0, 6), 1024, body(b"abc")).await.unwrap(), 3);
        assert_eq!(receive(path, &piece(3, 6), 1024, body(b"def")).await.unwrap(), 6);
        assert_eq!(std::fs::read(path).unwrap(), b"abcdef");

        // A gap, a rewind and an overshoot all produce the same refusal.
        assert!(receive(path, &piece(9, 12), 1024, body(b"xyz")).await.is_err());
        assert!(receive(path, &piece(3, 12), 1024, body(b"xyz")).await.is_err());
        assert!(receive(path, &piece(6, 7), 1024, body(b"toolong")).await.is_err());
        // None of them modified the file, so the upload can continue.
        assert_eq!(std::fs::metadata(path).unwrap().len(), 6);

        // The ceiling is checked against the declared total, before any bytes
        // arrive.
        assert!(receive(path, &piece(0, 4096), 1024, body(b"a")).await.is_err());
        assert!(receive(path, &piece(0, 0), 1024, body(b"")).await.is_err());

        // Starting over truncates whatever an interrupted attempt left behind.
        assert_eq!(receive(path, &piece(0, 2), 1024, body(b"hi")).await.unwrap(), 2);
        assert_eq!(std::fs::read(path).unwrap(), b"hi");
        std::fs::remove_file(path).unwrap();
    }

    /// Both scratch names a restore uses sit beside the live database, and one left
    /// behind is what the next upload fails on: `receive` refuses a first chunk
    /// that does not align with an existing file.
    ///
    /// This does not cover the rename in `db_restore`, which changes which path is
    /// open during the copy and would require a second upload landing inside it.
    /// It covers the part that outlives the request, which is what a later edit
    /// could silently drop.
    #[tokio::test]
    async fn a_finished_restore_leaves_no_scratch_file_behind() {
        let dir = std::env::temp_dir().join(format!("monitor-restore-{}", &random_token()[..16]));
        std::fs::create_dir_all(&dir).unwrap();
        let live = dir.join("live.db").to_string_lossy().into_owned();
        let app = std::sync::Arc::new(App::for_test(Db::open(&live).unwrap()));
        node(&app, "kept", true);

        // What a restore actually receives: a backup of a hub database.
        let copy = format!("{live}.copy");
        app.db.backup_into(&copy).unwrap();
        let bytes = std::fs::read(&copy).unwrap();
        std::fs::remove_file(&copy).unwrap();

        let done = db_restore(
            Admin,
            State(app.clone()),
            Query(Chunk { offset: 0, total: bytes.len() as u64 }),
            HeaderMap::new(),
            axum::body::Body::from(bytes),
        )
        .await;
        assert_eq!(done.status(), StatusCode::OK);
        assert_eq!(app.db.nodes().unwrap().len(), 1, "the backup went in");

        // The database and its journal are the only files that may remain.
        let left: Vec<String> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
            .filter(|name| !matches!(name.as_str(), "live.db" | "live.db-wal" | "live.db-shm"))
            .collect();
        assert!(left.is_empty(), "left beside the database: {left:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    /// A connected agent holding one report. The receiver is returned because
    /// dropping it closes the channel, which is the signal `reset_token` is tested
    /// for.
    fn connect(app: &App, id: i64, metrics: Value) -> tokio::sync::mpsc::Receiver<String> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let mut agent = crate::agent_ws::Agent::new(7, tx);
        agent.metrics = metrics;
        agent.last_seen = Utc::now().timestamp();
        app.agents.write().unwrap().insert(id, agent);
        rx
    }

    /// A probe assigned to `nodes`. The window query draws only a node's current
    /// assignments, so a fixture holding ping records requires one.
    fn task(app: &App, nodes: Vec<i64>) -> i64 {
        app.db
            .save_ping_task(&PingTask {
                id: 0,
                name: "probe".into(),
                target: "1.1.1.1:443".into(),
                interval: 60,
                nodes,
            })
            .unwrap()
    }

    fn node(app: &App, name: &str, public: bool) -> i64 {
        app.db
            .create_node(
                &Node { name: name.into(), public, remark: "secret note".into(), ..Default::default() },
                &format!("token-of-{name}"),
            )
            .unwrap()
    }

    /// A chart request costs roughly the same whatever it spans. This path
    /// requires no session, so an unbounded window would be megabytes of JSON any
    /// caller could have the hub build on the connection the agents report
    /// through.
    #[test]
    fn a_history_window_costs_the_same_however_wide_it_is() {
        let app = app();
        let id = node(&app, "n", true);
        let now = Utc::now().timestamp();
        // A month of history at the rate the hub writes it. Two probes, because
        // the budget is per series and a single-probe fixture would conceal that.
        const PROBES: i64 = 2;
        for _ in 0..PROBES {
            task(&app, vec![id]);
        }
        for i in 0..30 * 1440 {
            app.db.insert_metric(id, now - i * 60, &json!({"cpu": 1.0})).unwrap();
            for task in 1..=PROBES {
                app.db.insert_ping(id, task, now - i * 20, 42).unwrap();
            }
        }

        // Including windows that do not divide evenly, which are where a step
        // rounded the wrong way overruns.
        for hours in [1, 6, 13, 23, 24, 168, 2_160] {
            let step = sample_step(hours, None);
            let since = now - hours * 3_600;
            let metrics = app.db.metrics(id, since, step).unwrap();
            let (ping, _) = app.db.ping_records(id, since, step).unwrap();
            // Against the budget itself rather than whatever the step produced:
            // derived from the step, this would only demonstrate that division
            // works. One bucket of slack, as the window rarely divides evenly.
            let cap = 1_441;
            assert!(metrics.len() <= cap, "{hours}h returned {} metric rows", metrics.len());
            assert!(
                ping.len() <= cap * PROBES as usize,
                "{hours}h returned {} ping rows for {PROBES} probes",
                ping.len()
            );
            // Thinned, but neither empty nor reaching outside the window.
            assert!(!metrics.is_empty() && !ping.is_empty(), "{hours}h returned nothing");
            // A bucket the window opens partway through begins before it.
            assert!(
                metrics.iter().all(|m| m["ts"].as_i64().unwrap() >= since - step),
                "{hours}h reached back too far"
            );
        }
        // The widest window costs no more than a narrow one: unthinned, a month of
        // history is 43,200 rows.
        assert!(app.db.metrics(id, now - 2_160 * 3_600, sample_step(2_160, None)).unwrap().len() <= 1_441);

        // A day returns every minute it holds: thinning exists only for what the
        // screen cannot draw.
        assert_eq!(sample_step(24, Some(2_000)), 60, "a day of minutes fits under the ceiling");
        assert_eq!(sample_step(6, Some(2_000)), 60, "and so does six hours");

        // A caller may request less than the budget, never more: the ceiling
        // belongs to the hub, since this path takes no credentials.
        assert!(sample_step(24, Some(390)) > sample_step(24, None));
        assert_eq!(sample_step(24, Some(100_000)), sample_step(24, None));
        assert_eq!(sample_step(24, Some(0)), sample_step(24, Some(60)));

        // Requesting one half leaves the other empty rather than sending it: on
        // the day window that half was two thirds of the response.
        let series = |q: &str| serde_urlencoded::from_str::<Window>(q).unwrap().series;
        assert_eq!(series("hours=24&series=ping").as_deref(), Some("ping"));
        assert!(series("hours=24").is_none(), "no series means both, which is what curl gets");
    }

    /// What a thinned bucket may return. Keeping one row and discarding the rest
    /// made the seven-day chart integrate to twice the traffic the minutes hold,
    /// and drew a probe losing half its packets as an unbroken line.
    #[test]
    fn a_thinned_bucket_answers_with_its_mean_and_says_what_it_lost() {
        let app = app();
        let id = node(&app, "n", true);
        // Anchored on a bucket boundary, one whole bucket in the past. Anchored on
        // `now`, the rows would straddle the boundary depending on the second the
        // suite runs at.
        let base = Utc::now().timestamp() / 120 * 120 - 120;
        // One bucket: a quiet minute and a busy one, then a probe that answered
        // once and timed out three times.
        app.db.insert_metric(id, base + 10, &json!({"cpu": 0.0, "net_rx": 0})).unwrap();
        app.db.insert_metric(id, base + 70, &json!({"cpu": 40.0, "net_rx": 1_000})).unwrap();
        for _ in 0..3 {
            task(&app, vec![id]);
        }
        for (i, latency) in [30, -1, -1, -1].into_iter().enumerate() {
            app.db.insert_ping(id, 1, base + 10 + i as i64 * 20, latency).unwrap();
        }
        // A second probe that never answered, and a third that answered cleanly.
        app.db.insert_ping(id, 2, base + 10, -1).unwrap();
        app.db.insert_ping(id, 3, base + 10, 12).unwrap();

        let m = &app.db.metrics(id, base, 120).unwrap()[0];
        assert_eq!(m["cpu"], 20.0, "the bucket is its mean, not one row of it");
        assert_eq!(m["net_rx"], 500);
        assert_eq!(m["ts"], base, "stamped with the bucket, so every series shares a grid");

        // Keyed by task rather than index: the rows share a timestamp, so
        // `ORDER BY ts` leaves their order to SQLite.
        let (rows, window_loss) = app.db.ping_records(id, base, 120).unwrap();
        let probe = |task: i64| {
            rows.iter().find(|r| r["task_id"] == task).unwrap_or_else(|| panic!("no probe {task}"))
        };
        assert_eq!(probe(1)["latency"], 30, "the median of what answered, not of the timeouts");
        assert_eq!(probe(1)["loss"], 75);
        assert_eq!(probe(2)["latency"], json!(null), "a bucket that was all timeout has no latency");
        assert_eq!(probe(2)["loss"], 100);
        // One answer, so there is nothing for a band to span.
        assert!(probe(1).get("band").is_none(), "{:?}", probe(1));
        // A clean bucket carries no loss key, which is why the percentage rounds
        // up: the key's absence denotes no loss, so no loss must be the only way
        // to produce it.
        assert!(probe(3).get("loss").is_none(), "{:?}", probe(3));
        // Each probe has one bucket here, so the window and bucket figures agree
        // -- precisely the fixture shape that concealed the difference between
        // them. The test below separates the two.
        assert_eq!(window_loss["2"], 100.0);
        assert!(window_loss.get("3").is_none(), "a probe that lost nothing is left out");

        // One timeout in a bucket too large for it to reach a whole percent:
        // truncating would report the same as a clean bucket.
        let wide = node(&app, "wide", true);
        let wide_probe = task(&app, vec![wide]);
        let wide_base = base / 180 * 180;
        for i in 0..180 {
            app.db.insert_ping(wide, wide_probe, wide_base + i, if i == 0 { -1 } else { 20 }).unwrap();
        }
        let (rows, _) = app.db.ping_records(wide, wide_base, 180).unwrap();
        assert_eq!(rows.len(), 1, "the fixture has to be one bucket for this to mean anything");
        let row = &rows[0];
        assert_eq!(row["loss"], 1, "a bucket that lost one of 180 has not lost none");

        // What the band conveys: the median reading and the two extremes the
        // bucket reached. Drawing 20 alone would render a 40 ms swing as a flat
        // point.
        let jitter = node(&app, "jitter", true);
        let jitter_probe = task(&app, vec![jitter]);
        for (i, latency) in [10, 20, 50, 20, 20].into_iter().enumerate() {
            app.db.insert_ping(jitter, jitter_probe, wide_base + i as i64, latency).unwrap();
        }
        let row = &app.db.ping_records(jitter, wide_base, 180).unwrap().0[0];
        assert_eq!(row["latency"], 20, "the middle answer, not the mean of 24");
        assert_eq!(row["band"], json!([10, 50]));

        // An even count has no single middle value, so it is the mean of the two
        // straddling it. Every neighbouring pair differs, so selecting one rank
        // either way would yield 20 or 30 rather than 25.
        let even = node(&app, "even", true);
        let even_probe = task(&app, vec![even]);
        for (i, latency) in [40, 10, 30, 20].into_iter().enumerate() {
            app.db.insert_ping(even, even_probe, wide_base + i as i64, latency).unwrap();
        }
        assert_eq!(app.db.ping_records(even, wide_base, 180).unwrap().0[0]["latency"], 25);
    }

    /// What a window lost is the proportion of its samples lost, and only the hub
    /// can determine it: `close_bucket` divides within each bucket and keeps the
    /// quotient, so the denominators are gone by the time a reader sees the rows.
    /// Averaging the bucket percentages would weight a bucket holding one sample
    /// equally with one holding twelve, and unequal buckets are the ordinary case
    /// rather than an edge one. The window's first and last are partial by
    /// construction, and a probe that starts, stops, loses its node or skips a
    /// round on a slow resolver produces more.
    #[test]
    fn a_probe_reports_the_share_of_the_window_it_lost_not_the_mean_of_its_buckets() {
        let app = app();
        let id = node(&app, "n", true);
        let probe = task(&app, vec![id]);
        let base = Utc::now().timestamp() / 60 * 60 - 120;
        // A full minute at five seconds per round with no loss, then a minute
        // holding one sample, which was lost, before the probe stopped.
        for i in 0..12 {
            app.db.insert_ping(id, probe, base + i * 5, 20).unwrap();
        }
        app.db.insert_ping(id, probe, base + 60, -1).unwrap();

        let (rows, loss) = app.db.ping_records(id, base, 60).unwrap();
        let per_bucket: Vec<i64> = rows.iter().map(|r| r["loss"].as_i64().unwrap_or(0)).collect();
        assert_eq!(per_bucket, vec![0, 100], "the buckets are right about themselves");

        // Their mean is 50%, while one round of thirteen did not answer.
        let window = loss.get(probe.to_string()).and_then(|v| v.as_f64()).expect("this probe lost one");
        assert!((window - 100.0 / 13.0).abs() < 1e-9, "{window}");
        assert!(window < 8.0, "the window lost {window}%, not the 50% its buckets average to");
    }

    #[test]
    fn the_public_view_hides_private_nodes_and_sensitive_fields() {
        let app = app();
        let open = node(&app, "open", true);
        node(&app, "hidden", false);
        app.db.save_facts(open, &json!({"hostname": "vps-1"}), "198.51.100.9").unwrap();

        // A live report, so the public view has metrics to strip. `hostname` is
        // what a node token in the wrong hands can insert, and what the agent
        // repository could add to the contract.
        let _held = connect(
            &app,
            open,
            json!({"boot_id": "abc", "net_rx_total": 134_000_000_000i64, "cpu": 1.0,
                   "hostname": "db-prod-01", "ip": "203.0.113.7"}),
        );

        let public = visible_nodes(&app, false).unwrap();
        assert_eq!(public.len(), 1, "a node marked private must not be listed");
        assert_eq!(public[0]["name"], "open");
        // Disclosing the token would let any visitor impersonate the node.
        for hidden in ["ip", "remark", "hostname", "token"] {
            assert!(public[0].get(hidden).is_none(), "{hidden} must not be public");
        }
        assert!(
            !serde_json::to_string(&public).unwrap().contains("token-of-open"),
            "no node's token may appear anywhere in a public payload"
        );
        // Raw kernel counters would disclose the machine's lifetime traffic, and
        // anything the contract does not name is not published at all, the report
        // coming from a machine holding one node's token.
        for hidden in ["boot_id", "net_rx_total", "net_tx_total", "hostname", "ip"] {
            assert!(public[0]["metrics"].get(hidden).is_none(), "{hidden} must not be public");
        }
        assert_eq!(public[0]["metrics"]["cpu"], 1.0, "the rest of the report still goes out");

        let admin = visible_nodes(&app, true).unwrap();
        assert_eq!(admin.len(), 2);
        assert_eq!(admin[0]["ip"], "198.51.100.9");
        assert_eq!(admin[0]["remark"], "secret note");
    }

    #[tokio::test]
    async fn rotating_a_token_closes_the_session_the_old_one_opened() {
        let app = std::sync::Arc::new(app());
        let id = node(&app, "n", true);
        let mut rx = connect(&app, id, Value::Null);

        let response = reset_token(Admin, axum::extract::State(app.clone()), Path(id)).await;
        assert_eq!(response.status(), StatusCode::OK);
        // The agent loop selects on this receiver, so a closed channel is how it
        // learns to stop. `try_recv`, because `recv().await` on a channel
        // incorrectly left open would hang the suite rather than fail it.
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)),
            "the old agent's channel must be closed"
        );
        assert!(app.agents.read().unwrap().is_empty(), "the node must read as offline at once");
    }

    /// Deleting a node must reach the connection it opened, for the same reason
    /// rotating its token does, and more urgently: SQLite reassigns the freed id
    /// to the next node created. Left connected, the old machine reports under
    /// that id, so an undeployed node appears online with another machine's
    /// metrics, and its traffic and history are booked to it.
    #[tokio::test]
    async fn deleting_a_node_closes_its_session_so_the_next_id_does_not_inherit_it() {
        let app = std::sync::Arc::new(app());
        let old = node(&app, "old", true);
        let mut rx = connect(&app, old, json!({"cpu": 42.0}));

        assert_eq!(
            delete_node(Admin, axum::extract::State(app.clone()), Path(old)).await.status(),
            StatusCode::OK
        );
        assert!(
            matches!(rx.try_recv(), Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)),
            "the deleted node's agent must be told to go"
        );

        // SQLite reuses the id; nothing of the old machine may accompany it.
        let fresh = node(&app, "fresh", true);
        assert_eq!(fresh, old, "the fixture only means anything if the id is reused");
        let nodes = visible_nodes(&app, true).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0]["online"], json!(false), "a node nobody deployed is not online");
        assert_eq!(nodes[0]["metrics"], Value::Null, "and it has nobody else's metrics");
    }

    /// Both writers enforce the same limits. The create path formerly accepted a
    /// whole `Node` unchecked, leaving everything the update path refuses
    /// reachable by another route.
    #[tokio::test]
    async fn both_write_paths_refuse_the_same_out_of_range_values() {
        let app = std::sync::Arc::new(app());
        let id = node(&app, "n", true);
        for bad in [
            json!({"name": "x", "traffic_reset_day": 99}),
            json!({"name": "x", "price": -5.0}),
            json!({"name": "x", "traffic_limit": -1}),
        ] {
            let created = create_node(
                Admin,
                axum::extract::State(app.clone()),
                domain_headers(),
                Ok(Json(serde_json::from_value(bad.clone()).unwrap())),
            )
            .await;
            assert_eq!(created.status(), StatusCode::BAD_REQUEST, "create accepted {bad}");
            let updated = update_node(
                Admin,
                axum::extract::State(app.clone()),
                Path(id),
                Ok(Json(serde_json::from_value(bad.clone()).unwrap())),
            )
            .await;
            assert_eq!(updated.status(), StatusCode::BAD_REQUEST, "update accepted {bad}");
        }
        assert_eq!(app.db.nodes().unwrap().len(), 1, "nothing was created");
    }

    /// A stream outlives the request that opened it, so everything the handshake
    /// tested must be re-read rather than captured -- both answers, not one. The
    /// admin frame carries every node's token in the clear, and the public frame
    /// is what switching the status page off is meant to withdraw; a socket
    /// surviving either decision would continue sending what was withdrawn.
    #[test]
    fn a_stream_re_reads_both_answers_its_handshake_tested() {
        let app = app();
        let hash = sha256("live-token");
        app.db.create_session(&hash, Utc::now().timestamp() + 3_600).unwrap();

        assert_eq!(stream_audience(&app, Some(&hash)), Some(true), "a live session gets the admin frame");
        assert_eq!(stream_audience(&app, None), Some(false), "an anonymous stream gets the public one");

        // Signing out, another device revoking this one, a password change and a
        // restore all manifest as this row disappearing.
        app.db.drop_session(&hash).unwrap();
        assert_eq!(stream_audience(&app, Some(&hash)), None, "a revoked session must end its stream");

        // The other half. `live_ws` refuses a new anonymous connection from here
        // and `nodes` answers 401, so a stream that continued was the only
        // remaining route, for as long as the tab stayed open.
        app.db.create_session(&hash, Utc::now().timestamp() + 3_600).unwrap();
        app.db.set("public_page", "off").unwrap();
        assert_eq!(stream_audience(&app, None), None, "closing the status page must end anonymous streams");
        assert_eq!(stream_audience(&app, Some(&hash)), Some(true), "a signed-in operator still gets theirs");
    }

    #[test]
    fn the_shared_snapshot_keeps_the_two_audiences_apart() {
        let app = app();
        let open = node(&app, "open", true);
        node(&app, "hidden", false);
        app.db.save_facts(open, &json!({"hostname": "vps-1"}), "198.51.100.9").unwrap();

        let public = live_snapshot(&app, false);
        let admin = live_snapshot(&app, true);
        // Caching must never let one audience's payload reach the other.
        assert!(!public.as_str().contains("198.51.100.9"), "the public frame must carry no address");
        assert!(!public.as_str().contains("hidden"), "the public frame must carry no private node");
        assert!(admin.as_str().contains("198.51.100.9") && admin.as_str().contains("hidden"));

        // Two reads over unchanged data prove nothing, since a rebuild returns the
        // same bytes, so the data is modified first.
        node(&app, "late", true);
        assert_eq!(live_snapshot(&app, false), public, "the frame is reused, not rebuilt per viewer");
    }

    #[test]
    fn a_clock_stepping_backwards_does_not_pin_a_stale_frame() {
        let app = app();
        node(&app, "first", true);
        live_snapshot(&app, false);

        // NTP correcting a fresh boot leaves the cached stamp in the future, which
        // does not constitute a young frame.
        app.snapshot.lock().unwrap()[0].0 = Utc::now().timestamp_millis() + 60_000;
        node(&app, "added-after", true);
        assert!(live_snapshot(&app, false).as_str().contains("added-after"));
    }

    /// The panel sends only a name, and expects the node just added to appear in
    /// the frame it is already streaming.
    #[tokio::test]
    async fn a_node_added_from_the_panel_needs_only_a_name_and_shows_up_at_once() {
        let app = std::sync::Arc::new(app());
        node(&app, "existing", true);
        assert!(!live_snapshot(&app, true).as_str().contains("added"));

        let added: Node = serde_json::from_value(json!({"name": "added"})).unwrap();
        // The defaults the panel relies on by omitting them, `public` above all:
        // the alternative would publish a node that was never published.
        assert!(added.public);
        assert_eq!(added.billing_cycle, "monthly");
        assert_eq!(added.traffic_reset_day, 1);

        let created = create_node(Admin, State(app.clone()), domain_headers(), Ok(Json(added))).await;
        assert_eq!(created.status(), StatusCode::OK);
        // Frames are cached for nearly two seconds, so without dropping the cache
        // the node just added would disappear from the list.
        assert!(live_snapshot(&app, true).as_str().contains("added"));

        // A name consisting only of spaces is refused and leaves no node behind.
        let blank = Json(serde_json::from_value::<Node>(json!({"name": "   "})).unwrap());
        let refused = create_node(Admin, State(app.clone()), domain_headers(), Ok(blank)).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert_eq!(app.db.nodes().unwrap().len(), 2);
    }

    /// Every gate on the anonymous route, in the order a batch install encounters
    /// them: closed, wrong key, open, expired, closed manually.
    #[tokio::test]
    async fn registration_only_works_inside_a_window_the_panel_opened() {
        let app = std::sync::Arc::new(app());
        let register = |key: Option<&str>, name: &str| {
            let mut headers = domain_headers();
            if let Some(key) = key {
                headers.insert("authorization", format!("Bearer {key}").parse().unwrap());
            }
            agent_register(
                State(app.clone()),
                ConnectInfo("198.51.100.7:40000".parse().unwrap()),
                headers,
                name.to_owned(),
            )
        };

        // Nothing was opened, so no key is correct.
        assert_eq!(register(Some("guess"), "a").await.status(), StatusCode::FORBIDDEN);
        assert!(app.db.nodes().unwrap().is_empty());

        assert_eq!(open_register(Admin, State(app.clone()), domain_headers()).await.status(), StatusCode::OK);
        let key = app.db.get("register_key").unwrap();
        assert_eq!(register(Some("guess"), "a").await.status(), StatusCode::FORBIDDEN);
        assert_eq!(register(None, "a").await.status(), StatusCode::FORBIDDEN);
        assert!(app.db.nodes().unwrap().is_empty());

        let issued = register(Some(&key), "  web-01\n").await;
        assert_eq!(issued.status(), StatusCode::OK);
        let token = axum::body::to_bytes(issued.into_body(), usize::MAX).await.unwrap().to_vec();
        let token = String::from_utf8(token).unwrap();
        // The purpose of the route: what returned is a token an agent can connect
        // with, not merely a 200.
        let id = app.db.node_by_token(&token).unwrap().expect("token opens a node");
        let node = app.db.nodes().unwrap().into_iter().find(|n| n.id == id).unwrap();
        assert_eq!(node.name, "web-01");
        // Registered nodes take the panel's defaults rather than `Node::default()`.
        assert!(node.public);
        assert_eq!(node.traffic_reset_day, 1);

        // An hour later the same key is worthless, which is what makes leaving the
        // window open harmless.
        app.db.set("register_until", &(Utc::now().timestamp() - 1).to_string()).unwrap();
        assert_eq!(register(Some(&key), "b").await.status(), StatusCode::FORBIDDEN);

        // Reopened, then closed manually: the key from the open window stops
        // working.
        open_register(Admin, State(app.clone()), domain_headers()).await;
        let key = app.db.get("register_key").unwrap();
        assert_eq!(close_register(Admin, State(app.clone())).await.status(), StatusCode::NO_CONTENT);
        assert_eq!(register(Some(&key), "c").await.status(), StatusCode::FORBIDDEN);
        assert_eq!(app.db.nodes().unwrap().len(), 1);
    }

    /// The ceiling on the anonymous route: a leaked key cannot fill the table.
    #[tokio::test]
    async fn one_window_stops_registering_at_the_limit() {
        let app = std::sync::Arc::new(app());
        open_register(Admin, State(app.clone()), domain_headers()).await;
        let key = app.db.get("register_key").unwrap();
        for i in 0..REGISTER_LIMIT {
            node(&app, &format!("n{i}"), true);
        }
        let mut headers = domain_headers();
        headers.insert("authorization", format!("Bearer {key}").parse().unwrap());
        let refused = agent_register(
            State(app.clone()),
            ConnectInfo("198.51.100.7:40000".parse().unwrap()),
            headers,
            "one-too-many".to_owned(),
        )
        .await;
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        assert_eq!(app.db.nodes().unwrap().len() as i64, REGISTER_LIMIT);
    }

    #[test]
    fn a_node_view_carries_traffic_even_while_offline() {
        let app = app();
        let id = node(&app, "n", true);
        app.db.accumulate(id, "b", Some((100, 100))).unwrap();
        app.db.accumulate(id, "b", Some((900, 500))).unwrap();
        app.db.touch_seen(id, 1_700_000_000).unwrap();

        let view = &visible_nodes(&app, true).unwrap()[0];
        assert_eq!(view["online"], false);
        assert_eq!(view["metrics"], Value::Null);
        assert_eq!(view["total_rx"], 800, "traffic is stored, not derived from the live state");
        assert_eq!(view["total_tx"], 400);
        // The live entry went with the connection, so "offline since" must come
        // from the node row.
        assert_eq!(view["last_seen"], 1_700_000_000);
    }

    /// A capacity arrives twice -- once in the facts stored at the handshake, and
    /// again in every report -- and the two diverge as soon as a disk is mounted
    /// on a running machine, which the agent detects by re-reading its mount table
    /// every sample. Drawn from the stored copy, the card and the detail page
    /// showed the same host two different sizes until it reconnected.
    #[test]
    fn a_capacity_that_changed_since_the_handshake_is_the_reported_one() {
        let app = app();
        let id = node(&app, "n", true);
        // What the handshake stored: 30 GB of disk, 1 GB of swap.
        app.db
            .save_facts(
                id,
                &json!({"mem_total": 1_000, "swap_total": 1i64 << 30, "disk_total": 30i64 << 30}),
                "ip",
            )
            .unwrap();

        let offline = &visible_nodes(&app, true).unwrap()[0];
        assert_eq!(
            offline["disk_total"],
            30i64 << 30,
            "with nobody connected the stored facts are all there is"
        );

        // A 5 GB volume is mounted and swap is disabled. The same session, with no
        // second hello, so the stored facts do not change.
        let _held = connect(
            &app,
            id,
            json!({"mem_total": 1_000, "swap_total": 0, "disk_total": 35i64 << 30, "cpu": 1.0}),
        );
        let live = &visible_nodes(&app, true).unwrap()[0];
        assert_eq!(live["disk_total"], 35i64 << 30, "the report is the truth while the agent is connected");
        assert_eq!(live["swap_total"], 0, "swapoff means zero, not the gigabyte that was there at connect");
        assert_eq!(live["disk_total"], live["metrics"]["disk_total"], "one number, not two");
        assert_eq!(app.db.node(id).unwrap().unwrap().disk_total, 30i64 << 30, "and no extra write to get it");
    }

    /// `PUBLIC_HOURS` bounds one window; this bounds how many are built
    /// concurrently. Each holds the connection the agents report through for its
    /// entire scan, and the path takes no credentials -- the same arrangement
    /// `RELAY_GATE` and `PASSWORD_GATE` enforce on the other two anonymous paths
    /// that make this process work hard.
    #[tokio::test]
    async fn history_queries_past_the_gate_are_refused_rather_than_queued() {
        let app = std::sync::Arc::new(app());
        let id = node(&app, "n", true);
        let ask = || {
            metrics(
                State(app.clone()),
                HeaderMap::new(),
                Path(id),
                Query(Window { hours: 1, points: None, series: None }),
            )
        };

        // `acquire().await` rather than `try_acquire().expect`: other tests in
        // this binary hold a permit briefly while passing through `metrics`, and
        // an instant grab of all four raced them as the suite grew.
        let mut held = Vec::new();
        for _ in 0..HISTORY_SLOTS {
            held.push(HISTORY_GATE.acquire().await.expect("the gate never closes"));
        }
        assert_eq!(ask().await.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(held);
        assert_eq!(ask().await.status(), StatusCode::OK, "a finished query gives its slot back");

        // An unauthorised caller is told so rather than asked to retry later: the
        // gate sits behind the visibility check deliberately.
        app.db.set("public_page", "off").unwrap();
        let mut held = Vec::new();
        for _ in 0..HISTORY_SLOTS {
            held.push(HISTORY_GATE.acquire().await.expect("the gate never closes"));
        }
        assert_eq!(ask().await.status(), StatusCode::UNAUTHORIZED);
        drop(held);
    }

    /// A settings write lands whole or not at all. Changing the password drops
    /// every session and places the replacement cookie on the response, so a 400
    /// raised afterwards -- on a later key, in whatever order the map iterates --
    /// would sign the admin out of every device without explanation.
    #[tokio::test]
    async fn a_settings_write_is_all_or_nothing() {
        let app = std::sync::Arc::new(app());
        app.db.set("admin_password_hash", "the-old-hash").unwrap();
        let save = |body: Value| save_settings(Admin, State(app.clone()), HeaderMap::new(), Json(body));

        // BTreeMap order places the password first, which is the failing case.
        let refused = save(json!({"admin_password": "a-long-enough-one", "retention_days": "abc"})).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert_eq!(app.db.get("admin_password_hash").as_deref(), Some("the-old-hash"));

        // A correctly named key carrying the wrong type is refused rather than
        // discarded while the response reports success.
        let refused = save(json!({"public_page": false})).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        assert_eq!(app.db.get("public_page"), None);

        let saved = save(json!({"public_page": "off", "retention_days": "7"})).await;
        assert_eq!(saved.status(), StatusCode::OK);
        assert_eq!(app.db.get("retention_days").as_deref(), Some("7"));
    }

    /// The install script and the sign-in page keep separate counters: five
    /// machines started with a stale key is a misconfigured deploy, and a shared
    /// counter would lock the operator out of the panel for the lockout window.
    #[tokio::test]
    async fn a_wrong_registration_key_does_not_lock_the_sign_in_page() {
        let app = std::sync::Arc::new(app());
        app.db.set("register_key", "the-key").unwrap();
        app.db.set("register_until", &(Utc::now().timestamp() + 60).to_string()).unwrap();
        let mut headers = domain_headers();
        headers.insert("authorization", "Bearer wrong".parse().unwrap());
        let peer: std::net::SocketAddr = "198.51.100.7:9000".parse().unwrap();

        // The attempt after the last permitted one answers 429 rather than 403.
        for _ in 0..5 {
            let refused =
                agent_register(State(app.clone()), ConnectInfo(peer), headers.clone(), "n".into()).await;
            assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        }
        assert!(app.registrations.locked(peer.ip()), "the register route counts its own failures");
        assert!(!app.throttle.locked(peer.ip()), "and the panel's sign-in page is not one of them");
    }

    #[test]
    fn per_node_reads_follow_the_public_flag_and_the_public_page_switch() {
        let app = app();
        let open = node(&app, "open", true);
        let hidden = node(&app, "hidden", false);

        assert!(readable(&app, false, open), "a published node is readable by anyone");
        assert!(!readable(&app, false, hidden), "a private node is not");
        assert!(!readable(&app, false, 9999), "an unknown id is not");
        assert!(readable(&app, true, hidden), "the panel sees a private node");

        // Switching the public page off closes even a published node.
        app.db.set("public_page", "off").unwrap();
        assert!(!readable(&app, false, open));
        assert!(readable(&app, true, open), "and never closes it for the panel");
    }

    /// The window ceiling is a scan bound rather than a response bound: the
    /// thinning already limits the row count, while a quarter-year still reads
    /// every row behind it holding the write connection.
    #[tokio::test]
    async fn an_anonymous_history_window_stops_at_a_week() {
        let app = std::sync::Arc::new(app());
        let id = node(&app, "n", true);
        let now = Utc::now().timestamp();
        // One sample per day for a month, so a row's presence identifies its
        // window. The minute of slack keeps day seven clear of the 168-hour cutoff:
        // exactly on the boundary, a second elapsing between these inserts and the
        // query below would drop it and leave the count one short.
        for day in 0..30 {
            app.db.insert_metric(id, now - day * 86_400 + 60, &json!({"cpu": 1.0})).unwrap();
        }
        let ask = |hours| {
            let query = format!("hours={hours}&series=metrics");
            metrics(
                State(app.clone()),
                HeaderMap::new(),
                Path(id),
                Query(serde_urlencoded::from_str::<Window>(&query).unwrap()),
            )
        };
        let rows =
            |body: &str| serde_json::from_str::<Value>(body).unwrap()["metrics"].as_array().unwrap().len();

        let week = axum::body::to_bytes(ask(168).await.into_body(), usize::MAX).await.unwrap();
        assert_eq!(rows(std::str::from_utf8(&week).unwrap()), 8, "a week reaches back seven days");

        // Requesting the quarter year formerly available to an anonymous caller
        // returns the week: the extra rows exist, and reading them is the cost.
        let quarter = axum::body::to_bytes(ask(2_160).await.into_body(), usize::MAX).await.unwrap();
        assert_eq!(quarter, week, "an anonymous window past a week is clamped to one");
    }

    #[tokio::test]
    async fn changing_the_password_kills_other_sessions_but_not_the_caller() {
        let app = std::sync::Arc::new(app());
        let stale = random_token();
        app.db.create_session(&sha256(&stale), Utc::now().timestamp() + 3_600).unwrap();

        let body = Json(json!({"admin_password": "a-long-enough-password"}));
        let response = save_settings(Admin, axum::extract::State(app.clone()), HeaderMap::new(), body).await;

        assert!(!app.db.session_valid(&sha256(&stale)), "sessions must not outlive the old password");

        // The caller receives a replacement rather than being logged out by its
        // own password change.
        let cookie = response
            .headers()
            .get(axum::http::header::SET_COOKIE)
            .expect("a replacement session")
            .to_str()
            .unwrap();
        let token = cookie.split(';').next().unwrap().split('=').nth(1).unwrap();
        assert!(app.db.session_valid(&sha256(token)), "the replacement session must work");
    }

    /// The panel hides the delete button on the caller's own row, so the mark is
    /// all that prevents an admin from signing themselves out.
    #[tokio::test]
    async fn the_session_list_marks_the_caller_and_hides_expired_rows() {
        let app = std::sync::Arc::new(app());
        let (mine, theirs, stale) = (random_token(), random_token(), random_token());
        let now = Utc::now().timestamp();
        app.db.create_session(&sha256(&mine), now + 3_600).unwrap();
        app.db.create_session(&sha256(&theirs), now + 7_200).unwrap();
        app.db.create_session(&sha256(&stale), now - 1).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(header::COOKIE, format!("monitor_session={mine}").parse().unwrap());
        let body = axum::body::to_bytes(
            sessions(Admin, axum::extract::State(app.clone()), headers).await.into_body(),
            usize::MAX,
        )
        .await
        .unwrap();
        let rows: Vec<Value> = serde_json::from_slice(&body).unwrap();

        assert_eq!(rows.len(), 2, "an expired session is not a session");
        assert_eq!(rows[0]["id"], sha256(&theirs), "newest first");
        assert_eq!(rows[0]["current"], false);
        assert_eq!(rows[1]["id"], sha256(&mine));
        assert_eq!(rows[1]["current"], true, "the caller's own row must be marked");
        assert_eq!(rows[1]["created_at"].as_i64().unwrap(), now + 3_600 - 14 * 86_400);

        delete_session(Admin, axum::extract::State(app.clone()), Path(sha256(&theirs))).await;
        assert!(!app.db.session_valid(&sha256(&theirs)), "the deleted device is signed out");
        assert!(app.db.session_valid(&sha256(&mine)), "and nobody else is");
    }

    #[tokio::test]
    async fn a_short_password_is_refused_and_changes_nothing() {
        let app = std::sync::Arc::new(app());
        let live = random_token();
        app.db.create_session(&sha256(&live), Utc::now().timestamp() + 3_600).unwrap();

        let body = Json(json!({"admin_password": "short"}));
        let response = save_settings(Admin, axum::extract::State(app.clone()), HeaderMap::new(), body).await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert!(app.db.get("admin_password_hash").is_none(), "the password must not have changed");
        assert!(app.db.session_valid(&sha256(&live)), "a rejected change must not log anyone out");
    }

    /// Housekeeping clamps whatever it finds, so an unparsable value is not an
    /// error downstream: it silently means 7 days, in a field still displaying
    /// what was entered.
    #[tokio::test]
    async fn a_retention_window_that_would_never_apply_is_refused() {
        let app = std::sync::Arc::new(app());
        let put = |v: &str| {
            save_settings(
                Admin,
                State(app.clone()),
                HeaderMap::new(),
                Json(json!({"retention_days": v.to_owned()})),
            )
        };
        for junk in ["", "abc", "0", "-1", "9999"] {
            assert_eq!(put(junk).await.status(), StatusCode::BAD_REQUEST, "{junk:?}");
        }
        assert!(app.db.get("retention_days").is_none(), "a refused window must not be stored");
        assert_eq!(put("7").await.status(), StatusCode::OK);
        assert_eq!(app.db.get("retention_days").as_deref(), Some("7"));
    }

    /// What `settings` returns must be what `save_settings` accepts. The panel
    /// echoes the whole form back and the write is all-or-nothing, so one key
    /// returned in a form the write refuses fails the entire page, naming a field
    /// that was never edited.
    #[tokio::test]
    async fn a_fresh_hub_answers_settings_that_it_will_take_back() {
        let app = std::sync::Arc::new(app());
        let Json(read) = settings(Admin, State(app.clone())).await;
        assert_eq!(read["retention_days"], "7", "the default belongs in the answer, not in each caller");

        // Exactly what the panel sends, on a hub where nothing was ever set.
        let echoed = json!({
            "site_name": read["site_name"],
            "retention_days": read["retention_days"],
            "github_proxy": read["github_proxy"],
            "public_page": "on",
        });
        assert_eq!(
            save_settings(Admin, State(app.clone()), HeaderMap::new(), Json(echoed)).await.status(),
            StatusCode::OK,
            "a fresh hub's own settings must survive a round trip"
        );
        assert_eq!(app.db.retention_days(), 7, "and the stored window is the one that was shown");
    }

    #[tokio::test]
    async fn settings_never_hand_back_the_github_secret() {
        let app = app();
        app.db.set("github_client_secret", "super-secret").unwrap();
        app.db.set("github_client_id", "public-id").unwrap();

        let Json(body) = settings(Admin, axum::extract::State(std::sync::Arc::new(app))).await;
        assert_eq!(body["github_client_id"], "public-id");
        assert_eq!(body["github_secret_set"], true);
        assert!(body.get("github_client_secret").is_none());
        assert!(!body.to_string().contains("super-secret"));
    }

    // ---- plugins(U5) ----
    //
    // 上传走 router 级整调(oneshot),分层照抄 main.rs:POST /api/plugins 在
    // 8 MiB 的 merge 子 router 里,主 router 的 64 KiB 层在它之外。Multipart
    // 提取器还会在 tower 的层之上再套一层自己的 body limit(缺省 2 MiB),main
    // 用 DefaultBodyLimit 配平了它——这里照抄,否则 2 MiB 以上的包在测试里就
    // 先失败,而生产里也会(这是本分层测试真正抓过的 bug)。

    use tower::ServiceExt as _;

    /// 与 plugin.rs tests 相同的最小合法模块:memory + bump 分配器 + 恒返回 0
    /// 的 on_event。加载、启停、测试与日志全都用它。
    const MINIMAL_WAT: &str = r#"
(module
  (memory (export "memory") 1)
  (global $heap (mut i32) (i32.const 1024))
  (func (export "__alloc") (param $cap i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $cap)))
    (local.get $ptr))
  (func (export "on_event") (param i32 i32) (result i32) (i32.const 0)))"#;

    fn plugin_manifest(plugin_id: &str, abi_version: i64) -> String {
        format!(
            "plugin_id = \"{plugin_id}\"\nname = \"Test Plugin\"\nversion = \"1.0.0\"\n\
             abi_version = {abi_version}\nsubscribes = [\"expiry_soon\"]\n"
        )
    }

    /// 内存里打一个 tar.gz,entry 名与字节由调用方给。
    fn tarball(files: &[(&str, Vec<u8>)]) -> Vec<u8> {
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        {
            let mut builder = tar::Builder::new(&mut encoder);
            for (name, bytes) in files {
                let mut header = tar::Header::new_gnu();
                header.set_size(bytes.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder.append_data(&mut header, name, bytes.as_slice()).unwrap();
            }
            builder.into_inner().unwrap();
        }
        encoder.finish().unwrap()
    }

    /// 打一个 entry 名任意的 tar.gz:tar::Builder 拒绝写 `..` 与绝对路径,而要
    /// 防的正是绕过了 Builder 的包——把名字直接改在原始 tar 字节上(重算
    /// checksum)再压缩。
    fn tarball_with_entry_name(name: &str, bytes: &[u8]) -> Vec<u8> {
        use std::io::Write as _;
        assert!(name.len() < 100, "tar 的 name 字段只有 100 字节");
        let mut raw = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut raw);
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, "placeholder", bytes).unwrap();
            builder.into_inner().unwrap();
        }
        // GNU 头:name 在 0..100,checksum 在 148..156(算时按空格)。
        for (i, b) in raw[..100].iter_mut().enumerate() {
            *b = name.as_bytes().get(i).copied().unwrap_or(0);
        }
        for b in &mut raw[148..156] {
            *b = b' ';
        }
        let sum: u32 = raw[..512].iter().map(|&b| b as u32).sum();
        raw[148..156].copy_from_slice(format!("{:06o}\0 ", sum).as_bytes());

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&raw).unwrap();
        encoder.finish().unwrap()
    }

    fn plugin_archive(manifest: &str) -> Vec<u8> {
        tarball(&[
            ("plugin.toml", manifest.as_bytes().to_vec()),
            ("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap()),
        ])
    }

    /// 难压缩的字节:让 gzip 之后仍然超线,上限检查面对的是真实的体量。
    fn noise(len: usize) -> Vec<u8> {
        let mut state = 0x2545F4914F6CDD1Du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 33) as u8
            })
            .collect()
    }

    /// 一个带 Weak 回指的 App:dispatch_one 要 upgrade 回 App 读 fuel/timeout
    /// 的 setting,占位 Registry 里没有这个回指。顺带立一个会话——router 级
    /// 整调会跑 Admin 提取器,cookie 在 plugin_request 里带上。
    fn plugin_app() -> std::sync::Arc<App> {
        let app = std::sync::Arc::new(App::for_test(Db::open(":memory:").unwrap()));
        app.plugins.write().unwrap_or_else(|e| e.into_inner()).init(&app);
        app.db.create_session(&sha256("plugin-router-test"), Utc::now().timestamp() + 3_600).unwrap();
        app
    }

    /// 一个 multipart 请求,`plugin` 字段携带 tar.gz,cookie 过 Admin 提取器。
    fn plugin_request(archive: Vec<u8>) -> axum::extract::Request {
        let boundary = "monitor-plugin-test";
        let mut body = format!(
            "--{boundary}\r\n\
             content-disposition: form-data; name=\"plugin\"; filename=\"plugin.tar.gz\"\r\n\
             content-type: application/gzip\r\n\r\n"
        )
        .into_bytes();
        body.extend_from_slice(&archive);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let len = body.len();
        axum::extract::Request::builder()
            .method("POST")
            .uri("/api/plugins")
            .header(header::CONTENT_TYPE, format!("multipart/form-data; boundary={boundary}"))
            .header(header::COOKIE, "monitor_session=plugin-router-test")
            // 浏览器发 FormData 总带 Content-Length;tower 的 body limit 层靠它
            // 在读第一个字节之前拒绝超限包。
            .header(header::CONTENT_LENGTH, len)
            .body(axum::body::Body::from(body))
            .unwrap()
    }

    /// 与 main.rs 相同的分层:上传路由挂在 8 MiB 的 merge 子 router,主 router
    /// 的 64 KiB 层在它之外。一个超过 64 KiB 的包从这里活着走到 handler,证明
    /// 挂载的层放行了大包(挂在主 router 的 64 KiB 层之下就会 413)。
    fn upload_router(app: &Shared) -> axum::Router {
        let uploads = axum::Router::new()
            .route("/api/plugins", axum::routing::post(upload_plugin))
            .layer(tower_http::limit::RequestBodyLimitLayer::new(MAX_CHUNK))
            .layer(axum::extract::DefaultBodyLimit::max(MAX_CHUNK))
            .with_state(app.clone());
        axum::Router::new()
            .route("/api/nodes", axum::routing::get(nodes))
            .layer(tower_http::limit::RequestBodyLimitLayer::new(64 * 1024))
            .merge(uploads)
            .with_state(app.clone())
    }

    async fn upload(app: &Shared, archive: Vec<u8>) -> Response {
        upload_router(app).oneshot(plugin_request(archive)).await.unwrap()
    }

    /// 同一条路由,但 body limit 抬到能装下超限包:handler 自己的累计上限是
    /// router 之外的第二道防线(生产里 tower 的层先断流),只有抬高第一道才
    /// 测得到它。
    async fn upload_past_router_limit(app: &Shared, archive: Vec<u8>) -> Response {
        let router = axum::Router::new()
            .route("/api/plugins", axum::routing::post(upload_plugin))
            .layer(axum::extract::DefaultBodyLimit::max(64 << 20))
            .with_state(app.clone());
        router.oneshot(plugin_request(archive)).await.unwrap()
    }

    async fn body_of(response: Response) -> Value {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// 合法包上传即入库、默认不启用(KTD10),模块按字节原样保存,sha256 对得上,
    /// 列表带出 subscribes。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_uploaded_plugin_lands_disabled_in_the_table() {
        let app = plugin_app();
        let wasm = wat::parse_str(MINIMAL_WAT).unwrap();
        let archive = plugin_archive(&plugin_manifest("com.example.mailer", 1));

        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        let rows = app.db.list_plugins().unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!((row.plugin_id.as_str(), row.status.as_str()), ("com.example.mailer", "disabled"));
        assert!(!row.enabled, "上传后默认不启用");
        assert!(row.last_error.is_none(), "能加载的包不该带错误");
        assert_eq!(row.wasm_blob, wasm, "模块按字节原样保存");
        assert_eq!(row.wasm_sha256, hex::encode(Sha256::digest(&wasm)));
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(row.id));

        // 列表把 subscribes 从 manifest 解出来,前端画徽标不必再猜。
        let listed = body_of(list_plugins(Admin, State(app.clone())).await).await;
        assert_eq!(listed[0]["plugin_id"], "com.example.mailer");
        assert_eq!(listed[0]["subscribes"], json!(["expiry_soon"]));
        assert_eq!(listed[0]["status"], "disabled");
        assert!(listed[0].get("wasm_blob").is_none(), "列表不携带模块字节");
    }

    /// 每一种坏包都带着原因被拒,并且什么都不写:manifest 缺失、ABI 不符、
    /// plugin_id 含 ':',以及路径带 `..` 或绝对路径的包(名字直接改在 tar 头上,
    /// 绕过打包工具的好心)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bad_packages_are_refused_with_a_reason_and_store_nothing() {
        let app = plugin_app();
        let cases: Vec<(Vec<u8>, &str)> = vec![
            // 没有 plugin.toml。
            (tarball(&[("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap())]), "没有 plugin.toml"),
            // ABI 不符。
            (plugin_archive(&plugin_manifest("com.example.mailer", 2)), "abi_version"),
            // plugin_id 含 ':'(kv 命名空间的分隔符)。
            (plugin_archive(&plugin_manifest("com.example:mailer", 1)), "':'"),
            // 路径越出包外:`..` 与绝对路径。
            (
                tarball_with_entry_name(
                    "../plugin.toml",
                    plugin_manifest("com.example.mailer", 1).as_bytes(),
                ),
                "..",
            ),
            (
                tarball_with_entry_name(
                    "/etc/plugin.toml",
                    plugin_manifest("com.example.mailer", 1).as_bytes(),
                ),
                "绝对路径",
            ),
        ];
        for (archive, needle) in cases {
            let refused = upload(&app, archive).await;
            assert_eq!(refused.status(), StatusCode::BAD_REQUEST, "{needle}");
            let bytes = axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap();
            let text = String::from_utf8_lossy(&bytes);
            assert!(text.contains(needle), "响应应说明 `{needle}`,实际:{text}");
        }
        assert!(app.db.list_plugins().unwrap().is_empty(), "被拒的包一行都不写");
    }

    /// 重复的 plugin_id 是覆盖不是升级,拒绝并保留原行。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_duplicate_plugin_id_is_refused() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.a", 1))).await.status(),
            StatusCode::OK
        );
        let second = upload(&app, plugin_archive(&plugin_manifest("com.example.a", 1))).await;
        assert_eq!(second.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(second.into_body(), usize::MAX).await.unwrap();
        assert!(
            String::from_utf8_lossy(&bytes).contains("已存在"),
            "应说明如何处理:{}",
            String::from_utf8_lossy(&bytes)
        );
        assert_eq!(app.db.list_plugins().unwrap().len(), 1, "旧版本原样保留");
    }

    /// 编译不过的包也入库:status=disabled、原因在 last_error,作者在面板上
    /// 看到而不是从日志里找;对它启用得到的 400 一样带出原因。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_broken_wasm_lands_with_the_reason_and_cannot_be_enabled() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.broken", 1).into_bytes()),
            ("plugin.wasm", b"\0asm\xde\xad\xbe\xef".to_vec()),
        ]);
        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        let row = &app.db.list_plugins().unwrap()[0];
        assert_eq!(row.status, "disabled");
        assert!(row.last_error.as_deref().unwrap().contains("编译失败"), "{:?}", row.last_error);

        assert_eq!(
            enable_plugin(Admin, State(app.clone()), Path(row.id)).await.status(),
            StatusCode::BAD_REQUEST
        );
        let row = app.db.get_plugin(row.id).unwrap().unwrap();
        assert_eq!(row.status, "failed", "失败的启用要落库成 failed");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(row.id));
    }

    /// 一个 3 MiB 的合法包(多塞一块难压缩的填充)经过照抄 main.rs 的分层
    /// router 完整入库。体量一次跨过两道线:64 KiB(主 router 的层——上传路由
    /// 必须挂在 8 MiB 的 merge 子 router 上,挂错层这条请求就 413)和 2 MiB
    /// (Multipart 提取器自己的缺省 body limit——main 用 DefaultBodyLimit 配平
    /// 了它,漏配的话包在解析阶段就失败)。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_multi_mib_legal_package_uploads_through_the_merged_router() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.big", 1).into_bytes()),
            ("plugin.wasm", wat::parse_str(MINIMAL_WAT).unwrap()),
            ("assets/pad.bin", noise(3 * 1024 * 1024)),
        ]);
        assert!(archive.len() > 2 * 1024 * 1024, "fixture 必须跨过 2 MiB 的缺省 body limit");
        assert_eq!(upload(&app, archive).await.status(), StatusCode::OK);
        assert_eq!(app.db.list_plugins().unwrap().len(), 1);
    }

    /// 超过字节上限的包,两道防线各尽其职:router 的层先行断流(413),handler
    /// 的累计上限是它之外的第二道(400 带原因)——后者只有抬高前者才测得到。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_package_over_the_byte_cap_is_refused() {
        let app = plugin_app();
        let archive = tarball(&[
            ("plugin.toml", plugin_manifest("com.example.huge", 1).into_bytes()),
            ("plugin.wasm", noise(MAX_PLUGIN as usize + 1)), // 单 entry 仍在 16 MiB 内
        ]);
        assert!(archive.len() as u64 > MAX_PLUGIN);

        // 生产路径:8 MiB 的层先看到超限。
        assert_eq!(upload(&app, archive.clone()).await.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert!(app.db.list_plugins().unwrap().is_empty());

        // 抬高第一道之后,handler 自己的累计上限接住它。
        let refused = upload_past_router_limit(&app, archive).await;
        assert_eq!(refused.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(refused.into_body(), usize::MAX).await.unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("上限"), "{}", String::from_utf8_lossy(&bytes));
        assert!(app.db.list_plugins().unwrap().is_empty());
    }

    /// 启停生命周期:enable 写库又装内存,test 走完整执行路径拿回结果,
    /// disable 两头都摘掉,test 随之变成 400。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enable_test_and_disable_walk_the_full_lifecycle() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.lifecycle", 1))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;

        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        let row = app.db.get_plugin(id).unwrap().unwrap();
        assert!(row.enabled && row.status == "enabled");
        assert!(app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));

        // 测试通知:合成的 ExpirySoon 事件,返回真实执行结果。
        let tested = test_plugin(Admin, State(app.clone()), Path(id)).await;
        assert_eq!(tested.status(), StatusCode::OK);
        let body = body_of(tested).await;
        assert_eq!(body["plugin_id"], "com.example.lifecycle");
        assert_eq!(body["wasm_result"], "success");
        assert!(body["elapsed_ms"].as_u64().is_some());

        assert_eq!(disable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        let row = app.db.get_plugin(id).unwrap().unwrap();
        assert!(!row.enabled && row.status == "disabled");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));
        assert_eq!(test_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::BAD_REQUEST);

        // 不存在的行号是 404,不是 500 或静默成功。
        assert_eq!(
            enable_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            disable_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(test_plugin(Admin, State(app.clone()), Path(9999)).await.status(), StatusCode::NOT_FOUND);
    }

    /// 删除把三处状态一起收:db 行、kv 行、内存里的实例;不存在的行是 404。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn deleting_a_plugin_takes_its_kv_with_it() {
        let app = plugin_app();
        assert_eq!(
            delete_plugin(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.gone", 1))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((id, "bot_token".to_owned())),
                Json(json!({"value": "secret"})),
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(app.db.get("plugin.com.example.gone:bot_token").as_deref(), Some("secret"));

        assert_eq!(delete_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::NO_CONTENT);
        assert!(app.db.get_plugin(id).unwrap().is_none());
        assert_eq!(app.db.get("plugin.com.example.gone:bot_token"), None, "kv 行随插件删除");
        assert!(!app.plugins.read().unwrap_or_else(|e| e.into_inner()).is_loaded(id));
        assert_eq!(list_plugin_kv(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::NOT_FOUND);
    }

    /// kv 的往返与校验:key 非空、不含 ':'、不超 128 字节;value 不超 8 KiB
    /// (与 host_kv_set 同限);body 必须是 {"value": "..."}。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_kv_round_trips_and_refuses_the_same_things_the_host_does() {
        let app = plugin_app();
        assert_eq!(
            upload(&app, plugin_archive(&plugin_manifest("com.example.kv", 1))).await.status(),
            StatusCode::OK
        );
        let id = app.db.list_plugins().unwrap()[0].id;
        let put = |key: &str, value: Value| {
            set_plugin_kv(Admin, State(app.clone()), Path((id, key.to_owned())), Json(value))
        };

        assert_eq!(
            put("webhook", json!({"value": "https://example.com/hook"})).await.status(),
            StatusCode::OK
        );
        // 覆盖写同一个 key。
        assert_eq!(
            put("webhook", json!({"value": "https://example.com/other"})).await.status(),
            StatusCode::OK
        );
        let listed = body_of(list_plugin_kv(Admin, State(app.clone()), Path(id)).await).await;
        assert_eq!(listed, json!([{"key": "webhook", "value": "https://example.com/other"}]));

        // key 为空或含 ':',value 超限,body 形状不对:全部 400,且不落库。
        assert_eq!(put("   ", json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("a:b", json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        let long = "k".repeat(PLUGIN_KV_KEY_MAX + 1);
        assert_eq!(put(&long, json!({"value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        let big = "v".repeat(PLUGIN_KV_VALUE_MAX + 1);
        assert_eq!(put("k", json!({"value": big})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("k", json!({"not_value": "x"})).await.status(), StatusCode::BAD_REQUEST);
        assert_eq!(put("k", json!({"value": 42})).await.status(), StatusCode::BAD_REQUEST);
        // 校验失败的写一个都没落库。
        assert_eq!(app.db.plugin_kv("com.example.kv").unwrap().len(), 1);

        // 不存在的插件行是 404。
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((9999, "k".to_owned())),
                Json(json!({"value": "x"})),
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
    }

    /// 点号碰撞:`com.example` 与 `com.example.tg-notify` 的 kv 互相看不见,
    /// 删除前者也不动后者的行——前缀匹配止于 `:`。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn plugin_kv_namespaces_do_not_leak_across_dotted_prefixes() {
        let app = plugin_app();
        for plugin_id in ["com.example", "com.example.tg-notify"] {
            assert_eq!(
                upload(&app, plugin_archive(&plugin_manifest(plugin_id, 1))).await.status(),
                StatusCode::OK
            );
        }
        let rows = app.db.list_plugins().unwrap();
        // list_plugins 按 uploaded_at DESC 排,靠 plugin_id 找回行号。
        let id_of = |pid: &str| rows.iter().find(|r| r.plugin_id == pid).unwrap().id;
        let (a, b) = (id_of("com.example"), id_of("com.example.tg-notify"));

        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((a, "bot_token".to_owned())),
                Json(json!({"value": "of-a"}))
            )
            .await
            .status(),
            StatusCode::OK
        );
        // B 看不到 A 的 key,尽管 A 的 plugin_id 是 B 的前缀。
        assert_eq!(body_of(list_plugin_kv(Admin, State(app.clone()), Path(b)).await).await, json!([]));
        // B 自己写一个同名 key,也不覆盖 A 的。
        assert_eq!(
            set_plugin_kv(
                Admin,
                State(app.clone()),
                Path((b, "bot_token".to_owned())),
                Json(json!({"value": "of-b"}))
            )
            .await
            .status(),
            StatusCode::OK
        );
        assert_eq!(app.db.get("plugin.com.example:bot_token").as_deref(), Some("of-a"));

        // 删除 A 连带清掉它的 kv,B 的原样保留。
        assert_eq!(delete_plugin(Admin, State(app.clone()), Path(a)).await.status(), StatusCode::NO_CONTENT);
        assert_eq!(app.db.get("plugin.com.example:bot_token"), None);
        let left = body_of(list_plugin_kv(Admin, State(app.clone()), Path(b)).await).await;
        assert_eq!(left, json!([{"key": "bot_token", "value": "of-b"}]));
    }

    /// 派发日志按 plugin_id 过滤:两个插件各自测试过,每个的日志只有自己的。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_dispatch_log_is_filtered_per_plugin() {
        let app = plugin_app();
        for plugin_id in ["com.example.alpha", "com.example.beta"] {
            assert_eq!(
                upload(&app, plugin_archive(&plugin_manifest(plugin_id, 1))).await.status(),
                StatusCode::OK
            );
        }
        let rows = app.db.list_plugins().unwrap();
        let id_of = |pid: &str| rows.iter().find(|r| r.plugin_id == pid).unwrap().id;
        let (alpha, beta) = (id_of("com.example.alpha"), id_of("com.example.beta"));
        for id in [alpha, beta, alpha] {
            assert_eq!(enable_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
            assert_eq!(test_plugin(Admin, State(app.clone()), Path(id)).await.status(), StatusCode::OK);
        }

        let log = body_of(plugin_dispatch_log(Admin, State(app.clone()), Path(alpha)).await).await;
        let entries = log.as_array().unwrap();
        assert_eq!(entries.len(), 2, "alpha 测试了两次:{log}");
        assert!(entries.iter().all(|e| e["plugin_id"] == "com.example.alpha"), "{log}");
        assert!(
            entries.iter().all(|e| e["result"] == "success" && e["event_type"] == "expiry_soon"),
            "{log}"
        );
        // 快照新 → 旧:最新一条在头部(两次测试可能落在同一秒,只比先后)。
        assert!(entries[0]["at"].as_i64() >= entries[1]["at"].as_i64(), "{log}");

        assert_eq!(
            plugin_dispatch_log(Admin, State(app.clone()), Path(9999)).await.status(),
            StatusCode::NOT_FOUND
        );
    }
}

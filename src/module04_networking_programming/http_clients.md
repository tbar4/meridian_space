# Lesson 3 — HTTP Clients with `reqwest`: Async REST Calls to Meridian's Mission API

**Module:** Foundation — M04: Network Programming  
**Position:** Lesson 3 of 3  
**Source:** Synthesized from `reqwest` documentation and training knowledge

> **Source note:** This lesson synthesizes from `reqwest` 0.12.x API documentation and training knowledge. Verify connection pool configuration options against the current `reqwest::ClientBuilder` docs if behaviour differs.

---
<!-- toc -->
---
## Context

The Meridian control plane is not an island. It fetches TLE updates from the external Space-Track catalog API, posts conjunction alerts to the mission operations REST endpoint, and retrieves ground station configuration from an internal config service. All of these are HTTP calls — outbound, async, with retry logic and timeouts.

`reqwest` is the standard async HTTP client for Rust. It wraps `hyper` (the underlying HTTP implementation) with a high-level, ergonomic API, built-in connection pooling, JSON support through `serde`, and configurable timeout and retry behaviour. Understanding how to use it correctly — particularly how `Client` is shared, how connection pools work, and how to handle failures robustly — is essential for any Rust service that communicates with external APIs.

---

## Core Concepts

### `Client` — Shared, Pooled, Long-Lived

`reqwest::Client` manages a connection pool internally. Building a `Client` is expensive — it allocates the pool, sets up TLS configuration, and resolves DNS configuration. A `Client` is designed to be created once and cloned cheaply for sharing across tasks.

```rust,edition2021
use reqwest::Client;
use std::time::Duration;

fn build_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        // Overall request timeout: connection + headers + body.
        .timeout(Duration::from_secs(30))
        // How long to wait for the TCP connection to establish.
        .connect_timeout(Duration::from_secs(5))
        // Keep connections alive for reuse — avoids TCP handshake per request.
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(10)
        // User-Agent header for all requests.
        .user_agent("meridian-control-plane/1.0")
        .build()?)
}
```

`Client` is `Clone` — cloning it is a reference count increment that shares the same underlying connection pool. Pass a `Client` to tasks by cloning, not by wrapping in `Arc<Mutex<Client>>`. The `Arc` is already inside `Client`.

Never create a new `Client` per request. Each new `Client` is a new connection pool — you lose all the benefit of connection reuse and accumulate resource overhead proportional to your request rate.

### Making Requests

The basic request pattern: call a method on the `Client` to get a `RequestBuilder`, add headers and body, call `.send().await`, check the status, and deserialize the response:

```rust,edition2021
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct TleRecord {
    norad_id: u32,
    name: String,
    line1: String,
    line2: String,
}

async fn fetch_tle(client: &Client, norad_id: u32) -> anyhow::Result<TleRecord> {
    let url = format!("https://api.meridian.internal/tle/{norad_id}");
    let response = client
        .get(&url)
        .header("X-API-Key", "mission-control-key")
        .send()
        .await?;

    // error_for_status() converts 4xx/5xx responses into Err.
    // Without this, a 404 or 500 is not an error — you receive the body.
    let response = response.error_for_status()?;

    let record: TleRecord = response.json().await?;
    Ok(record)
}
```

`error_for_status()` is important. A 404 or 503 does not cause `.send().await` to return `Err` — only network errors do. If you omit `error_for_status()`, a 500 response body is deserialized as if it were a valid `TleRecord`, producing a confusing JSON parse error rather than a clear HTTP error.

### Sending JSON Bodies

For POST and PUT requests with JSON bodies, use `.json(&value)` on the `RequestBuilder`. It serializes the value with serde, sets the `Content-Type: application/json` header, and sets the body:

```rust,edition2021
use reqwest::Client;
use serde::Serialize;

#[derive(Serialize)]
struct ConjunctionAlert {
    object_a: u32,
    object_b: u32,
    tca_seconds: f64,
    miss_distance_km: f64,
}

async fn post_alert(client: &Client, alert: &ConjunctionAlert) -> anyhow::Result<()> {
    client
        .post("https://api.meridian.internal/alerts")
        .json(alert)
        .send()
        .await?
        .error_for_status()?;
    Ok(())
}
```

`.json()` requires the `json` feature on `reqwest` (enabled by default). For large payloads that should be streamed rather than buffered in memory, use `.body(reqwest::Body::wrap_stream(stream))` instead.

### Retry Logic with Exponential Backoff

External APIs fail transiently — rate limits, brief outages, transient DNS failures. A single retry with a fixed delay is rarely sufficient. Exponential backoff with jitter is the standard approach: wait 1s, then 2s, then 4s, with random jitter to avoid thundering herds:

```rust,edition2021
use reqwest::{Client, StatusCode};
use tokio::time::{sleep, Duration};

async fn fetch_with_retry(
    client: &Client,
    url: &str,
    max_attempts: u32,
) -> anyhow::Result<String> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let result = client.get(url).send().await;

        match result {
            Ok(resp) if resp.status().is_success() => {
                return Ok(resp.text().await?);
            }
            Ok(resp) if resp.status() == StatusCode::TOO_MANY_REQUESTS => {
                // Respect Retry-After header if present, otherwise backoff.
                let retry_after = resp
                    .headers()
                    .get("Retry-After")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<u64>().ok())
                    .unwrap_or(0);
                let delay = if retry_after > 0 {
                    Duration::from_secs(retry_after)
                } else {
                    backoff_delay(attempt)
                };
                tracing::warn!(attempt, url, ?delay, "rate limited — backing off");
                if attempt >= max_attempts { anyhow::bail!("rate limit exhausted"); }
                sleep(delay).await;
            }
            Ok(resp) if resp.status().is_server_error() => {
                tracing::warn!(attempt, url, status = %resp.status(), "server error");
                if attempt >= max_attempts {
                    anyhow::bail!("server error after {max_attempts} attempts");
                }
                sleep(backoff_delay(attempt)).await;
            }
            Ok(resp) => {
                // 4xx client errors (except 429) are not retryable.
                anyhow::bail!("request failed: HTTP {}", resp.status());
            }
            Err(e) if e.is_connect() || e.is_timeout() => {
                tracing::warn!(attempt, url, "network error: {e}");
                if attempt >= max_attempts { return Err(e.into()); }
                sleep(backoff_delay(attempt)).await;
            }
            Err(e) => return Err(e.into()),
        }
    }
}

fn backoff_delay(attempt: u32) -> Duration {
    // Exponential backoff: 1s, 2s, 4s, 8s, capped at 30s.
    // Add jitter to avoid thundering herd.
    use std::time::SystemTime;
    let base = Duration::from_secs(1u64 << attempt.min(5));
    let jitter_ms = (SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis()) % 1000;
    base + Duration::from_millis(jitter_ms as u64)
}
```

Retry strategy by status code:
- **5xx (server error):** Retry with backoff — transient server issues.
- **429 (too many requests):** Retry with backoff, respect `Retry-After` header.
- **408 (request timeout) or connection/timeout errors:** Retry with backoff.
- **4xx (client errors) except 429:** Do not retry — the request itself is malformed.
- **Success:** Return immediately.

### Configuring Timeouts Correctly

A single `.timeout(Duration)` sets the overall request timeout (connection + sending + receiving). For fine-grained control:

```rust,edition2021
use reqwest::Client;
use std::time::Duration;

fn build_production_client() -> anyhow::Result<Client> {
    Ok(Client::builder()
        // TCP connection timeout — fail fast if service is unreachable.
        .connect_timeout(Duration::from_secs(3))
        // Total time budget for the entire request (all phases).
        .timeout(Duration::from_secs(15))
        // How long an idle connection can sit in the pool before being closed.
        .pool_idle_timeout(Duration::from_secs(60))
        .build()?)
}
```

For the Meridian TLE catalog API — a slow external service that can take up to 10 seconds to respond during load — set the timeout to 12–15 seconds. For the internal mission ops REST endpoint on the same datacenter network, 3–5 seconds is appropriate. Do not use the same `Client` configuration for both if the timeout requirements differ significantly — build two clients.

---

## Code Examples

### TLE Catalog HTTP Client for the Control Plane

The control plane fetches TLE updates from Space-Track on a 10-minute schedule. It also exposes a REST endpoint for on-demand TLE queries. This example shows both directions: fetching and posting, with retry logic and a shared client.

```rust,edition2021
use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Deserialize, Clone)]
pub struct TleRecord {
    pub norad_id: u32,
    pub name: String,
    pub line1: String,
    pub line2: String,
    pub epoch: String,
}

#[derive(Debug, Serialize)]
pub struct ConjunctionReport {
    pub object_a_id: u32,
    pub object_b_id: u32,
    pub tca_unix: f64,
    pub miss_distance_km: f64,
    pub probability: f64,
}

pub struct MissionApiClient {
    client: Client,
    base_url: String,
    api_key: String,
}

impl MissionApiClient {
    pub fn new(base_url: String, api_key: String) -> Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .pool_max_idle_per_host(4)
            .user_agent("meridian-control-plane/1.0")
            .build()
            .context("failed to build HTTP client")?;
        Ok(Self { client, base_url, api_key })
    }

    /// Fetch a single TLE record with up to 3 retry attempts.
    pub async fn get_tle(&self, norad_id: u32) -> Result<TleRecord> {
        let url = format!("{}/tle/{norad_id}", self.base_url);
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let response = self.client
                .get(&url)
                .header("X-API-Key", &self.api_key)
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    return resp.json::<TleRecord>().await
                        .context("failed to parse TLE response");
                }
                Ok(resp) if resp.status().is_server_error() && attempt < 3 => {
                    tracing::warn!(norad_id, attempt, status = %resp.status(), "retrying");
                    sleep(Duration::from_secs(1 << attempt)).await;
                }
                Ok(resp) => {
                    anyhow::bail!("TLE fetch failed: HTTP {}", resp.status());
                }
                Err(e) if (e.is_connect() || e.is_timeout()) && attempt < 3 => {
                    tracing::warn!(norad_id, attempt, "network error: {e}, retrying");
                    sleep(Duration::from_secs(1 << attempt)).await;
                }
                Err(e) => return Err(e).context("TLE fetch network error"),
            }
        }
    }

    /// Post a conjunction report to the mission operations endpoint.
    pub async fn post_conjunction(&self, report: &ConjunctionReport) -> Result<()> {
        self.client
            .post(format!("{}/conjunctions", self.base_url))
            .header("X-API-Key", &self.api_key)
            .json(report)
            .send()
            .await
            .context("failed to send conjunction report")?
            .error_for_status()
            .context("conjunction report rejected")?;
        Ok(())
    }

    /// Fetch all active TLEs in a specified altitude band (batch request).
    pub async fn get_tle_batch(&self, min_km: u32, max_km: u32) -> Result<Vec<TleRecord>> {
        self.client
            .get(format!("{}/tle/batch", self.base_url))
            .query(&[("min_alt_km", min_km), ("max_alt_km", max_km)])
            .header("X-API-Key", &self.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<TleRecord>>()
            .await
            .context("failed to parse TLE batch response")
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let api = MissionApiClient::new(
        "https://api.meridian.internal".to_string(),
        "mission-control-key".to_string(),
    )?;

    // Periodic TLE refresh loop.
    let api_ref = std::sync::Arc::new(api);
    let refresh_api = std::sync::Arc::clone(&api_ref);

    tokio::spawn(async move {
        loop {
            match refresh_api.get_tle(25544).await {
                Ok(tle) => tracing::info!(name = %tle.name, "TLE refreshed"),
                Err(e) => tracing::error!("TLE refresh failed: {e}"),
            }
            sleep(Duration::from_secs(600)).await;
        }
    });

    // Post a conjunction report.
    api_ref.post_conjunction(&ConjunctionReport {
        object_a_id: 25544,
        object_b_id: 48274,
        tca_unix: 1_735_000_000.0,
        miss_distance_km: 0.8,
        probability: 0.003,
    }).await?;

    sleep(Duration::from_secs(1)).await;
    Ok(())
}
```

The `MissionApiClient` wraps the `reqwest::Client` and encodes the API contract — base URL, auth header, response types — in one place. Callers interact with typed methods rather than raw HTTP primitives. The `Arc::new(api)` pattern is appropriate here because `Client` is already internally reference-counted; wrapping in `Arc` just lets the `MissionApiClient` struct itself be shared. A simpler option is to pass `&MissionApiClient` to async functions directly, since `MissionApiClient` is `Send + Sync`.

---

## Key Takeaways

- Create one `Client` per configuration profile and share it across tasks via `Clone`. Each new `Client` is a new connection pool — creating one per request wastes connection setup overhead and defeats pooling.

- Always call `error_for_status()` after `.send().await` unless you explicitly want to handle 4xx/5xx response bodies. HTTP error responses do not return `Err` from `send()`.

- Use `.json(&value)` for serializing request bodies with serde. Use `.json::<T>()` on the response for deserialization. Both require the `json` feature (enabled by default).

- Distinguish retryable errors (5xx, 429, connection/timeout errors) from non-retryable ones (4xx client errors). Apply exponential backoff with jitter for retryable failures. Respect `Retry-After` headers on 429 responses.

- Set `connect_timeout` separately from the overall `.timeout`. A short connect timeout (3–5s) fails fast on unreachable services without waiting for the full request timeout budget.

- For different external services with different latency profiles and rate limits, use separate `Client` instances with separate configurations rather than sharing one client across everything.

---

{{#quiz lesson-03-quiz.toml}}
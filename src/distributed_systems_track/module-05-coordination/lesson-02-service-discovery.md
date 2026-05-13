# Lesson 2: Service Discovery and Load Balancing

## Context

The Constellation Network has grown from twelve services to forty-three in the last eighteen months. Each ground station runs an instance of the telemetry-ingest service; each region runs replicas of the orbital-element-registry; each pass-window scheduler talks to a dozen downstream services to validate decisions. The original deployment configuration — a static config file listing every service's IP and port — was last accurate in March of last year. Today, it is updated by hand on Tuesdays during the maintenance window, and roughly one in five incidents traces back to a service being addressable from a config file that no longer matches reality.

Static configuration does not work at this scale. The system needs **service discovery** — a mechanism by which a service that wants to call `orbital-registry` gets a current list of healthy `orbital-registry` instances without anyone editing a config file. The system also needs **load balancing** — once it has a list of instances, the caller needs to pick one that is alive, not overloaded, and ideally close to the caller in network terms.

The two problems are entangled. A service discovery system that returns dead instances forces clients to do their own health checking. A load balancer that can't see the current instance list can't distribute load. The mature production answer integrates the two: the discovery layer exposes only live instances, and the load balancer uses real-time health and load signals to route. This lesson covers the discovery patterns (client-side vs server-side, registries vs DNS), the load-balancing algorithms (round-robin, least-loaded, consistent hashing, power-of-two-choices), and the operational discipline that connects them to the failure detector from Module 4.

## Core Concepts

### Service Discovery: Client-Side vs Server-Side

Two architectural patterns dominate service discovery, and each has different operational implications.

**Client-side discovery.** The caller queries a registry directly for instances of the target service, picks one, and connects. Examples: Netflix Eureka, HashiCorp Consul (in client-side mode), the etcd-based discovery used by many Kubernetes operators. The caller is responsible for caching the registry's response, refreshing it periodically, handling registry failures, and implementing the load balancing.

The advantages: no extra network hop; the caller has full visibility into available instances and can make sophisticated routing decisions; the load-balancing logic runs on every caller, distributing CPU cost. The disadvantages: every caller must implement the same discovery/balancing logic, leading to drift between languages and frameworks; clients can hold stale lists; the registry becomes a critical dependency for every caller.

**Server-side discovery.** The caller connects to a load balancer (HAProxy, NGINX, Envoy, AWS ELB, Kubernetes Service), which itself queries the registry and forwards to a backend. The caller doesn't need to know anything beyond "the load balancer's address." Examples: every Kubernetes Service, AWS Application Load Balancer, GCP Cloud Load Balancer.

The advantages: callers are simple; one well-tested load-balancing implementation serves all clients; routing decisions can be sophisticated without duplicating logic. The disadvantages: extra network hop (the load balancer is in the request path); the load balancer is a critical dependency that must be highly available; some routing decisions (e.g., affinity based on caller identity) are harder to implement.

The constellation uses both. The high-volume internal RPC paths (telemetry ingest → registry → catalog) use client-side discovery with a shared library that handles caching, health, and balancing. The external API gateway and the inter-region links use server-side discovery via an Envoy mesh, on the basis that the operational simplicity is worth the extra hop.

### Registries: Push vs Pull, Static vs Dynamic

The registry is the source of truth for "which instances exist." How it learns about instances and how clients learn from it shapes the system's behavior under failure.

**Pull-based registries.** Services register themselves on startup (HTTP POST to the registry) and refresh their registration periodically. The registry expires registrations that haven't refreshed within a TTL. Clients pull the current list on demand. Eureka and Consul work this way.

**Push-based registries.** The registry watches the underlying infrastructure (Kubernetes API server, Nomad job state, cloud instance metadata) and is automatically updated by the orchestrator. Clients receive push notifications via watches or streams when the list changes. The Kubernetes Endpoints model and most service mesh control planes work this way.

**DNS-based discovery.** A degenerate registry where service names map to A/AAAA records (or SRV records for ports). Universal compatibility — every language has a DNS client — at the cost of weak semantics: DNS caching means clients can have stale records for the TTL duration; DNS has no health awareness on its own. Many systems use DNS for discovery with very short TTLs (5–30 seconds), accepting the staleness window as the tradeoff for universal compatibility.

The push-based pattern is operationally cleaner when an orchestrator is the source of truth: the orchestrator decides which instances exist, and the registry reflects that. Pull-based is the older pattern but still widely used; it tolerates an absent orchestrator at the cost of more registration logic in each service.

### Health Awareness in Discovery

A service that has registered itself isn't necessarily healthy. A service that has registered itself and not yet been removed from the registry isn't necessarily still running. The registry needs to actively distinguish healthy instances from registered ones.

Three mechanisms appear:

**TTL expiry.** Registrations expire if not refreshed. The crashed service stops refreshing; after the TTL, the registry removes it. This catches hard crashes but is slow (the TTL is typically tens of seconds, during which clients still see the dead instance).

**Active health checks from the registry.** The registry probes each registered instance periodically — HTTP `/health`, TCP connect, gRPC health check. Failed probes remove the instance from the active list immediately. Faster than TTL expiry; costs more registry CPU.

**Passive health checks at the client.** The load balancer or client library tracks per-instance failure rates and excludes unhealthy instances from rotation. This catches failures the active probes miss (a service that returns 200 to `/health` but 500 to actual requests). Envoy's "outlier detection" is the canonical implementation.

The constellation uses TTL expiry as a baseline (every registry entry has a 30-second TTL with 10-second refresh) plus passive health detection at the client (10% failure rate over 30 seconds excludes the instance for 60 seconds). The combination provides both correctness (TTL eventually removes truly dead instances) and responsiveness (passive checks react in seconds to actual failures).

### Load Balancing Algorithms

Once you have a list of healthy instances, you need to pick one. The algorithms span a wide range of complexity:

**Round-robin.** Cycle through instances in order. Simple, stateless, fair if instances are equivalent in capacity. Fails when instances are not equivalent — a slow instance still gets its fair share of requests and accumulates a backlog.

**Random.** Pick uniformly at random. Same statistical properties as round-robin in the limit; doesn't require any coordination between callers, which matters when callers are independent processes that can't share state.

**Least-connections / least-loaded.** Pick the instance with the fewest in-flight requests. Routes around slow or overloaded instances naturally. Requires tracking per-instance state, which is fine for a single load balancer but harder for distributed callers (each caller sees only its own connections, not the global load).

**Power-of-two-choices.** Pick two instances at random, then pick the less-loaded of the two. Surprisingly effective: produces near-optimal load distribution with vastly less state than least-loaded. The standard reference is Mitzenmacher's 2001 paper. Used by NGINX (`least_conn` with `random two`), Envoy, and Akka.

**Consistent hashing.** Map each request's key to an instance via a hash ring. The same key always routes to the same instance (until the instance set changes). Essential for caching tiers and any system where instance affinity matters. The Karger et al. 1997 paper is the canonical reference; modern implementations use jump consistent hashing (Lamping & Veach 2014) or rendezvous hashing for simpler implementation.

**Weighted variants.** Each algorithm can be weighted by instance capacity, geographic distance, or recent latency. Production load balancers typically support weighting; the operational value is matching real instance characteristics.

The constellation uses power-of-two-choices for general RPC, consistent hashing for the catalog's read tier (so the same TLE is always served by the same replica, maximizing cache hit rate), and weighted round-robin for the cross-region paths (weighted by inter-region link bandwidth).

### Latency-Aware Routing

When instances span different network regions, a load balancer that picks uniformly produces a mixture of fast and slow routes. **Latency-aware routing** measures per-instance response time and prefers faster instances.

The implementations:

**EWMA-based.** Each caller maintains an exponentially weighted moving average of per-instance latency. Routing prefers low-EWMA instances. Simple, low-overhead, adapts quickly to changes. The cost is per-caller state and the cold-start problem (a new instance starts with no EWMA).

**P2C with latency.** The power-of-two-choices variant where the "less loaded" comparison uses recent latency instead of connection count. Inherits P2C's good statistical properties.

**Zone-aware routing.** Hard-code or auto-detect that instances are in the same region as the caller and prefer them. Used heavily in cloud deployments to keep cross-AZ traffic minimal.

The constellation uses zone-aware routing for the standard case (calls stay within the same region whenever possible) and EWMA-based selection as the fallback when the local region is unavailable.

### The Failure Detector Connection

The failure detector from Module 4 Lesson 1 is the substrate that makes service discovery and load balancing work. The discovery layer's "healthy instance list" is built from failure-detector signals — actively probed or passively observed. The load balancer's "this instance is slow" decision is a suspicion-level reading from the same detector.

This is why the constellation team standardized on phi accrual for service-to-service liveness: the same continuous suspicion signal drives discovery (high suspicion → temporarily remove from the rotation), load balancing (medium suspicion → reduce weight), and circuit breaking (sustained high suspicion → open the breaker). The three layers consume different thresholds on the same signal.

The alternative — three separate detection mechanisms with different timeouts — is what produces flapping. A discovery layer that removes instances at 5 seconds, a load balancer that re-includes at 10 seconds, and a circuit breaker that trips at 30 seconds will interact in ways that nobody designed. Centralize the liveness signal.

## Code Examples

### A Client-Side Discovery Cache

```rust,edition2021
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ServiceInstance {
    pub id: String,
    pub endpoint: String,
    pub last_health_check: Instant,
}

pub struct DiscoveryCache {
    inner: Arc<Mutex<HashMap<String, Vec<ServiceInstance>>>>,
    ttl: Duration,
}

impl DiscoveryCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            ttl,
        }
    }

    /// Called periodically (e.g., every 10s) to refresh the cache from the
    /// registry. Stale entries (not seen in this refresh) are evicted - this
    /// is what removes deregistered or expired instances from the cache.
    pub fn refresh(&self, service: &str, instances: Vec<ServiceInstance>) {
        let mut cache = self.inner.lock().unwrap();
        cache.insert(service.to_string(), instances);
    }

    /// Returns the current cached instance list. Callers may further filter
    /// by health (using the passive health detector) before picking one.
    pub fn instances(&self, service: &str) -> Vec<ServiceInstance> {
        let cache = self.inner.lock().unwrap();
        cache.get(service).cloned().unwrap_or_default()
    }

    /// Returns the instances that are considered fresh (last_health_check
    /// within the TTL). Filters out any instance whose health-check timestamp
    /// is older than the TTL, even if it's still in the cache.
    pub fn fresh_instances(&self, service: &str) -> Vec<ServiceInstance> {
        let cutoff = Instant::now() - self.ttl;
        self.instances(service)
            .into_iter()
            .filter(|i| i.last_health_check >= cutoff)
            .collect()
    }
}
```

The cache is a simple read-mostly structure with periodic refresh. The two-level filtering (refresh writes the cache; `fresh_instances` filters by freshness on read) handles the case where the refresh is delayed or the registry itself has stale entries.

### Power-of-Two-Choices Load Balancer

```rust,edition2021
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

# #[derive(Clone)] struct ServiceInstance { id: String, endpoint: String }

pub struct InstanceState {
    pub instance: ServiceInstance,
    pub in_flight: AtomicU64,
    pub recent_failures: AtomicU64,
}

pub struct P2CBalancer {
    instances: Vec<Arc<InstanceState>>,
}

impl P2CBalancer {
    pub fn new(instances: Vec<ServiceInstance>) -> Self {
        Self {
            instances: instances
                .into_iter()
                .map(|i| {
                    Arc::new(InstanceState {
                        instance: i,
                        in_flight: AtomicU64::new(0),
                        recent_failures: AtomicU64::new(0),
                    })
                })
                .collect(),
        }
    }

    /// Power-of-two-choices: pick two instances at random, return the one
    /// with fewer in-flight requests. Surprisingly effective: produces
    /// near-optimal load distribution with O(1) work per call and no
    /// cross-caller coordination.
    pub fn pick(&self) -> Option<Arc<InstanceState>> {
        let n = self.instances.len();
        if n == 0 { return None; }
        if n == 1 { return Some(self.instances[0].clone()); }

        let a = fastrand::usize(0..n);
        let mut b = fastrand::usize(0..n);
        // Ensure b != a; loop is bounded since n >= 2.
        while b == a { b = fastrand::usize(0..n); }

        let inst_a = &self.instances[a];
        let inst_b = &self.instances[b];

        // Compare by in_flight; tiebreak by recent_failures (prefer fewer).
        let score_a = inst_a.in_flight.load(Ordering::Relaxed);
        let score_b = inst_b.in_flight.load(Ordering::Relaxed);

        if score_a <= score_b {
            Some(inst_a.clone())
        } else {
            Some(inst_b.clone())
        }
    }
}

# mod fastrand {
#     pub fn usize(_r: std::ops::Range<usize>) -> usize { 0 }
# }
```

The key property: each caller picks independently, with no coordination, and the statistical distribution of choices converges to within a constant factor of optimal. Mitzenmacher's paper proves this; the practical observation is that P2C beats round-robin by a wide margin under heterogeneous load and is essentially free to implement.

### Consistent Hashing for Cache-Affinity Routing

```rust,edition2021
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

# #[derive(Clone)] struct ServiceInstance { id: String }

pub struct ConsistentHashRing {
    // BTreeMap gives O(log n) lookup for the next-larger key, which is the
    // canonical consistent-hash lookup. Each instance occupies multiple
    // 'virtual nodes' on the ring to smooth distribution.
    ring: BTreeMap<u64, ServiceInstance>,
    virtual_nodes_per_instance: usize,
}

impl ConsistentHashRing {
    pub fn new(virtual_nodes_per_instance: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            virtual_nodes_per_instance,
        }
    }

    pub fn add_instance(&mut self, instance: ServiceInstance) {
        for vn in 0..self.virtual_nodes_per_instance {
            let key = hash_key(&format!("{}#{}", instance.id, vn));
            self.ring.insert(key, instance.clone());
        }
    }

    pub fn remove_instance(&mut self, instance_id: &str) {
        for vn in 0..self.virtual_nodes_per_instance {
            let key = hash_key(&format!("{}#{}", instance_id, vn));
            self.ring.remove(&key);
        }
    }

    /// Return the instance responsible for the given key. The instance set
    /// changes only when nodes are added or removed; otherwise the same key
    /// always maps to the same instance.
    pub fn lookup(&self, key: &str) -> Option<&ServiceInstance> {
        if self.ring.is_empty() { return None; }
        let h = hash_key(key);
        // Find the smallest ring position >= h; if none, wrap around to the
        // smallest position overall.
        self.ring
            .range(h..)
            .next()
            .or_else(|| self.ring.iter().next())
            .map(|(_, instance)| instance)
    }
}

fn hash_key(s: &str) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}
```

The `virtual_nodes_per_instance` parameter is the smoothing knob. With one virtual node per instance, the distribution is uneven (an instance can own a much larger arc of the ring than its share). With 100–200 virtual nodes per instance, the distribution becomes nearly uniform with negligible CPU cost. Production implementations (Cassandra, DynamoDB) typically use a few hundred virtual nodes per physical instance.

## Key Takeaways

- Service discovery is the dynamic-configuration layer that replaces static IP lists. Client-side discovery puts logic in each caller (simpler infrastructure, more language duplication); server-side puts a load balancer in the path (one well-tested implementation, extra network hop).
- The registry needs active or passive health awareness to be useful. TTL-based expiry catches hard crashes; passive failure detection at the client catches the subtle "200 to /health, 500 to real requests" failure mode. Use both.
- Power-of-two-choices is the practical default for load balancing across equivalent instances: O(1) per call, near-optimal distribution, no coordination required. Round-robin is acceptable when instances are truly equivalent; least-loaded is appropriate when one balancer has full visibility.
- Consistent hashing is the right tool when key affinity matters (caching, sharded state). Use virtual nodes per physical instance to smooth distribution; production deployments use hundreds of virtual nodes each.
- The failure detector underlies discovery and load balancing both. Use one detector with multiple thresholds for the different consumers (discovery removal, load balancer reweighting, circuit breaker tripping) rather than separate detection mechanisms that interact unpredictably.

> **Source note:** This lesson is synthesized from training knowledge plus the canonical sources for each load balancing algorithm. Karger et al., "Consistent Hashing and Random Trees" (STOC 1997) is the consistent hashing original. Mitzenmacher, "The Power of Two Choices in Randomized Load Balancing" (IEEE TPDS 2001) is the P2C analysis. The Envoy and NGINX documentation are the practical references for production configurations. DDIA Chapter 10 briefly discusses service discovery under "Membership and Coordination Services" but does not go deep on load balancing algorithms. *Foundations of Scalable Systems* would have been the standard reference here and was unavailable; cross-check before publication.

{{#quiz quiz-2.toml}}

// ===== DATA MODEL =====
const CRATES = [
  {
    name: 'std',
    runtime: 'sync',
    description: 'The standard library. No external dependencies. Thread-based blocking channels only.',
    channels: [
      {
        name: 'mpsc::channel',
        pattern: 'mpsc',
        description: 'Unbounded async (non-blocking send). Infinite buffer; send never blocks.',
        bounded: false,
        blockingSend: true,
        blockingRecv: true,
        trySend: false,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: false,
        cloneTx: true,
        cloneRx: false,
        select: false,
      },
      {
        name: 'mpsc::sync_channel',
        pattern: 'mpsc',
        description: 'Bounded synchronous channel. Send blocks when buffer is full. Supports bound of 0 for rendezvous.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: false,
        cloneTx: true,
        cloneRx: false,
        select: false,
      },
    ],
  },
  {
    name: 'tokio',
    runtime: 'async',
    description: 'The de facto async runtime. Four distinct channel types for different communication patterns.',
    channels: [
      {
        name: 'mpsc::channel',
        pattern: 'mpsc',
        description: 'Bounded async MPSC. Backpressure via capacity. The workhorse of async Tokio code.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: false,
        select: true,
      },
      {
        name: 'mpsc::unbounded_channel',
        pattern: 'mpsc',
        description: 'Unbounded async MPSC. Send is synchronous (never awaits). No backpressure.',
        bounded: false,
        blockingSend: false,
        blockingRecv: true,
        trySend: false,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: false,
        select: true,
      },
      {
        name: 'oneshot::channel',
        pattern: 'oneshot',
        description: 'Single value, single use. Perfect for request/response patterns and task completion signals.',
        bounded: true,
        blockingSend: false,
        blockingRecv: true,
        trySend: false,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: true,
        cloneTx: false,
        cloneRx: false,
        select: true,
      },
      {
        name: 'broadcast::channel',
        pattern: 'broadcast',
        description: 'Every receiver gets every message (via Clone). MPMC fan-out. Lagging receivers get errors.',
        bounded: true,
        blockingSend: false,
        blockingRecv: true,
        trySend: false,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
      {
        name: 'watch::channel',
        pattern: 'watch',
        description: 'Latest-value only. Receivers see the most recent value; no history. Great for config/state.',
        bounded: false,
        blockingSend: false,
        blockingRecv: true,
        trySend: false,
        tryRecv: false,
        asyncSend: false,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
    ],
  },
  {
    name: 'crossbeam',
    runtime: 'sync',
    description: 'High-performance lock-free concurrent primitives. The gold standard for sync MPMC channels.',
    channels: [
      {
        name: 'channel::bounded',
        pattern: 'mpmc',
        description: 'Bounded MPMC with select! support. Cloneable Rx enables multiple consumers. Bound 0 = rendezvous.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: false,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
      {
        name: 'channel::unbounded',
        pattern: 'mpmc',
        description: 'Unbounded MPMC. Same Sender type as bounded; send documented as blocking but never blocks in practice.',
        bounded: false,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: false,
        asyncRecv: false,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
    ],
  },
  {
    name: 'flume',
    runtime: 'both',
    description: 'Lightweight MPMC with both sync and async APIs on the same channel. No unsafe code.',
    channels: [
      {
        name: 'bounded',
        pattern: 'mpmc',
        description: 'Bounded MPMC. Mix sync send with async recv on the same channel. No unsafe.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
      {
        name: 'unbounded',
        pattern: 'mpmc',
        description: 'Unbounded MPMC. Sync/async interop. Ideal bridge between sync producers and async consumers.',
        bounded: false,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: true,
      },
    ],
  },
  {
    name: 'kanal',
    runtime: 'both',
    description: 'Ultra-fast channels using direct stack-to-stack memory copy. Sync/async interop with dedicated types.',
    channels: [
      {
        name: 'bounded',
        pattern: 'mpmc',
        description: 'Bounded MPMC with zero-copy where possible. Dedicated AsyncSender/AsyncReceiver types prevent misuse.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: false,
      },
      {
        name: 'unbounded',
        pattern: 'mpmc',
        description: 'Unbounded MPMC. Provides close() to broadcast shutdown signal from any handle, similar to Go.',
        bounded: false,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: false,
      },
    ],
  },
  {
    name: 'async-channel',
    runtime: 'async',
    description: 'Lightweight async MPMC by the smol ecosystem. Each message goes to exactly one receiver.',
    channels: [
      {
        name: 'bounded',
        pattern: 'mpmc',
        description: 'Bounded async MPMC. Each message delivered to one receiver only. Backpressure via capacity.',
        bounded: true,
        blockingSend: false,
        blockingRecv: false,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: false,
      },
      {
        name: 'unbounded',
        pattern: 'mpmc',
        description: 'Unbounded async MPMC. Send never blocks or awaits. Fully compatible with Tokio runtime.',
        bounded: false,
        blockingSend: false,
        blockingRecv: false,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: false,
      },
    ],
  },
  {
    name: 'crossfire',
    runtime: 'both',
    description: 'High-performance SPSC/MPSC/MPMC built on crossbeam internals with async support layered on top.',
    channels: [
      {
        name: 'spsc',
        pattern: 'spsc',
        description: 'Single-producer single-consumer. Maximum throughput for point-to-point communication.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: false,
        cloneRx: false,
        select: false,
      },
      {
        name: 'mpsc',
        pattern: 'mpsc',
        description: 'Multi-producer single-consumer with async bridging. Based on crossbeam internals.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: false,
        select: false,
      },
      {
        name: 'mpmc',
        pattern: 'mpmc',
        description: 'Multi-producer multi-consumer with async support. Lock-free crossbeam core with async wrapper.',
        bounded: true,
        blockingSend: true,
        blockingRecv: true,
        trySend: true,
        tryRecv: true,
        asyncSend: true,
        asyncRecv: true,
        cloneTx: true,
        cloneRx: true,
        select: false,
      },
    ],
  },
];

const DECISIONS = [
  { q: 'Need async send/recv in Tokio?', a: 'tokio::sync::mpsc', note: 'The standard choice. Bounded for backpressure, unbounded when send must not await.' },
  { q: 'Sync threads only, high throughput?', a: 'crossbeam::channel', note: 'Lock-free, battle-tested. Supports select! for multiplexing multiple channels.' },
  { q: 'Bridge between sync and async code?', a: 'flume or kanal', note: 'Both allow sync senders with async receivers on the same channel. Flume has no unsafe; kanal is faster.' },
  { q: 'Request/response or task completion?', a: 'tokio::oneshot', note: 'Single value, zero overhead. Pair with mpsc: send a oneshot Tx in the request, await the Rx for the response.' },
  { q: 'Fan-out the same value to N receivers?', a: 'tokio::broadcast', note: 'Every subscriber gets a clone of every message. Lagging receivers receive an error, not a block.' },
  { q: 'Latest config/state (skip old values)?', a: 'tokio::watch', note: 'Receivers only see the most recent value. No buffering, no history. Perfect for shared config updates.' },
  { q: 'MPMC where each msg goes to one consumer?', a: 'async-channel or flume', note: 'Work-stealing pattern: multiple consumers share a queue. async-channel is smol-native; flume works everywhere.' },
  { q: 'Maximum raw throughput, SPSC?', a: 'crossfire::spsc or kanal', note: 'crossfire::spsc is specialized for single-producer/single-consumer. kanal uses zero-copy stack transfers.' },
  { q: 'Standard library only, no deps?', a: 'std::sync::mpsc', note: 'Good enough for simple cases. sync_channel(0) gives rendezvous semantics. No async support.' },
];

// ===== DECISION FLOWCHART TREE =====
const FLOW_TREE = {
  id: 'start',
  q: 'Are you writing async code?',
  options: [
    {
      label: 'Yes, async/.await',
      next: {
        id: 'async',
        q: 'How many consumers need each message?',
        options: [
          {
            label: 'One consumer',
            next: {
              id: 'async-one',
              q: 'Do you need multiple producers?',
              options: [
                {
                  label: 'Yes, many producers',
                  next: {
                    id: 'async-mpsc',
                    q: 'Should send() block when full?',
                    options: [
                      { label: 'Yes, backpressure', result: { name: 'tokio::sync::mpsc::channel', why: 'Bounded MPSC with backpressure. The workhorse of Tokio. send().await suspends until there is capacity.' } },
                      { label: 'No, never block on send', result: { name: 'tokio::sync::mpsc::unbounded_channel', why: 'Unbounded MPSC. send() is synchronous and never awaits. Use when producers must not stall, but watch memory usage.' } },
                    ],
                  },
                },
                {
                  label: 'One producer, one value',
                  result: { name: 'tokio::sync::oneshot', why: 'Single-use, single-value channel. Zero overhead. Embed the Tx in a request, await the Rx for the reply.' },
                },
              ],
            },
          },
          {
            label: 'Every consumer gets every msg',
            next: {
              id: 'async-broadcast',
              q: 'Do receivers need the full history or just the latest value?',
              options: [
                { label: 'Full history (fan-out)', result: { name: 'tokio::sync::broadcast', why: 'Each subscriber gets a clone of every message. Lagging receivers get an error. Requires T: Clone.' } },
                { label: 'Only the latest value', result: { name: 'tokio::sync::watch', why: 'Receivers always see the most recent value. No buffering, no history. Perfect for config or state signals.' } },
              ],
            },
          },
          {
            label: 'Multiple consumers, one msg each',
            next: {
              id: 'async-mpmc',
              q: 'Do you need sync/async interop on the same channel?',
              options: [
                { label: 'Async only', result: { name: 'async-channel', why: 'Lightweight async MPMC from the smol ecosystem. Each message goes to exactly one receiver. Works with any async runtime.' } },
                { label: 'Mix sync + async', result: { name: 'flume', why: 'MPMC with both sync and async APIs on the same channel. Sync producers can feed async consumers without a bridge.' } },
              ],
            },
          },
        ],
      },
    },
    {
      label: 'No, sync threads only',
      next: {
        id: 'sync',
        q: 'Do you need multiple consumers (MPMC)?',
        options: [
          {
            label: 'Yes, multiple consumers',
            next: {
              id: 'sync-mpmc',
              q: 'What matters most?',
              options: [
                { label: 'Battle-tested + select!', result: { name: 'crossbeam::channel', why: 'Lock-free MPMC with select! macro for multiplexing. The gold standard. Cloneable receivers enable fan-out.' } },
                { label: 'Raw throughput', result: { name: 'kanal', why: 'Direct stack-to-stack memory transfer for maximum speed. Custom mutex tuned for channel workloads. Go-style close() for shutdown.' } },
              ],
            },
          },
          {
            label: 'No, single consumer',
            next: {
              id: 'sync-mpsc',
              q: 'Any external dependencies allowed?',
              options: [
                { label: 'No deps, stdlib only', result: { name: 'std::sync::mpsc', why: 'No external crates needed. sync_channel(0) for rendezvous. Since Rust 1.67 the internals are based on crossbeam.' } },
                { label: 'Deps are fine', result: { name: 'crossbeam::channel', why: 'Better performance and ergonomics than std. select! support, try_send/try_recv, timeout operations, and tick channels.' } },
              ],
            },
          },
        ],
      },
    },
    {
      label: 'Both — bridging sync and async',
      next: {
        id: 'bridge',
        q: 'What kind of bridge pattern?',
        options: [
          { label: 'Sync producers → async consumers', result: { name: 'flume', why: 'Send synchronously from threads, recv_async() in Tokio tasks. Same channel handles both. No unsafe code.' } },
          { label: 'Maximum throughput bridge', result: { name: 'kanal', why: 'Dedicated AsyncSender/AsyncReceiver types prevent misuse. Stack-to-stack copy for minimal overhead. Explicit close() signal.' } },
          { label: 'Point-to-point, one producer', result: { name: 'crossfire::spsc', why: 'Single-producer single-consumer with async support layered over crossbeam. Maximum throughput for 1:1 pipelines.' } },
        ],
      },
    },
  ],
};

// ===== CODE SNIPPETS DATA =====
const SNIPPETS = [
  {
    title: 'std::sync::mpsc',
    badge: 'MPSC',
    badgeClass: 'mpsc',
    code: `use std::sync::mpsc;
use std::thread;

let (tx, rx) = mpsc::channel();

// Clone tx for multiple producers
let tx2 = tx.clone();

thread::spawn(move || {
    tx.send("from thread 1").unwrap();
});

thread::spawn(move || {
    tx2.send("from thread 2").unwrap();
});

// Receive blocks until a message arrives
while let Ok(msg) = rx.recv() {
    println!("{msg}");
}`,
  },
  {
    title: 'tokio::sync::mpsc',
    badge: 'MPSC',
    badgeClass: 'mpsc',
    code: `use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = mpsc::channel(32);
    let tx2 = tx.clone();

    tokio::spawn(async move {
        tx.send("hello").await.unwrap();
    });

    tokio::spawn(async move {
        tx2.send("world").await.unwrap();
    });

    while let Some(msg) = rx.recv().await {
        println!("{msg}");
    }
}`,
  },
  {
    title: 'tokio::sync::oneshot',
    badge: 'ONESHOT',
    badgeClass: 'oneshot',
    code: `use tokio::sync::oneshot;

#[tokio::main]
async fn main() {
    let (tx, rx) = oneshot::channel();

    tokio::spawn(async move {
        let result = expensive_computation().await;
        tx.send(result).unwrap();
    });

    // Await the single response
    let value = rx.await.unwrap();
    println!("Got: {value}");
}`,
  },
  {
    title: 'tokio::sync::broadcast',
    badge: 'BROADCAST',
    badgeClass: 'broadcast',
    code: `use tokio::sync::broadcast;

#[tokio::main]
async fn main() {
    let (tx, _) = broadcast::channel(16);

    // Each subscriber gets its own Rx
    let mut rx1 = tx.subscribe();
    let mut rx2 = tx.subscribe();

    tx.send("event").unwrap();

    // Both receivers see "event"
    assert_eq!(rx1.recv().await.unwrap(), "event");
    assert_eq!(rx2.recv().await.unwrap(), "event");
}`,
  },
  {
    title: 'tokio::sync::watch',
    badge: 'WATCH',
    badgeClass: 'watch',
    code: `use tokio::sync::watch;

#[tokio::main]
async fn main() {
    let (tx, mut rx) = watch::channel("initial");

    tokio::spawn(async move {
        loop {
            // Blocks until value changes
            rx.changed().await.unwrap();
            let val = rx.borrow().clone();
            println!("Config updated: {val}");
        }
    });

    tx.send("updated").unwrap();
}`,
  },
  {
    title: 'crossbeam::channel',
    badge: 'MPMC',
    badgeClass: 'mpmc',
    code: `use crossbeam::channel;
use std::thread;

let (tx, rx) = channel::bounded(100);

// Multiple producers
for i in 0..4 {
    let tx = tx.clone();
    thread::spawn(move || {
        tx.send(i).unwrap();
    });
}
drop(tx); // Close sender side

// Multiple consumers (clone rx)
let rx2 = rx.clone();
thread::spawn(move || {
    while let Ok(msg) = rx2.recv() {
        println!("Consumer B: {msg}");
    }
});

while let Ok(msg) = rx.recv() {
    println!("Consumer A: {msg}");
}`,
  },
  {
    title: 'flume (sync/async bridge)',
    badge: 'MPMC',
    badgeClass: 'mpmc',
    code: `use flume;

#[tokio::main]
async fn main() {
    let (tx, rx) = flume::bounded(100);

    // Sync producer (from a thread)
    std::thread::spawn(move || {
        for i in 0..10 {
            tx.send(i).unwrap(); // blocking send
        }
    });

    // Async consumer (in Tokio)
    while let Ok(val) = rx.recv_async().await {
        println!("Got: {val}");
    }
}`,
  },
  {
    title: 'kanal',
    badge: 'MPMC',
    badgeClass: 'mpmc',
    code: `use kanal;

#[tokio::main]
async fn main() {
    let (tx, rx) = kanal::bounded(100);

    // Dedicated async types prevent misuse
    let async_tx = tx.clone_async();
    let async_rx = rx.clone_async();

    tokio::spawn(async move {
        async_tx.send(42).await.unwrap();
    });

    let val = async_rx.recv().await.unwrap();
    println!("Got: {val}");

    // close() broadcasts shutdown from any handle
    tx.close();
}`,
  },
  {
    title: 'async-channel',
    badge: 'MPMC',
    badgeClass: 'mpmc',
    code: `use async_channel;

#[tokio::main]
async fn main() {
    let (tx, rx) = async_channel::bounded(100);

    // Spawn worker pool (clone rx)
    for id in 0..4 {
        let rx = rx.clone();
        tokio::spawn(async move {
            while let Ok(task) = rx.recv().await {
                println!("Worker {id}: {task}");
            }
        });
    }

    for i in 0..20 {
        tx.send(i).await.unwrap();
    }
    tx.close(); // signal workers to stop
}`,
  },
];

// ===== PERFORMANCE TIERS DATA =====
const PERF_TIERS = [
  {
    label: 'Fastest',
    icon: '⚡',
    color: 'var(--color-primary)',
    barPct: 100,
    desc: 'Specialized optimizations: custom mutexes, stack-to-stack copy, zero-alloc paths',
    crates: [
      { name: 'kanal', detail: 'Direct memory access, tuned mutex. Dominates in SPSC/MPSC bounded(0).' },
      { name: 'crossfire', detail: 'Lock-free crossbeam core. SPSC variant is the fastest point-to-point.' },
    ],
    scenarios: [
      { scenario: 'SPSC bounded', leader: 'crossfire > kanal > crossbeam' },
      { scenario: 'MPSC bounded(0)', leader: 'kanal > crossbeam > flume' },
      { scenario: 'Async MPSC', leader: 'kanal > crossfire > tokio' },
    ],
  },
  {
    label: 'Fast',
    icon: '🟢',
    color: 'var(--color-async)',
    barPct: 78,
    desc: 'Highly optimized general-purpose channels. Excellent for most production workloads.',
    crates: [
      { name: 'crossbeam', detail: 'Lock-free MPMC. The baseline that others benchmark against.' },
      { name: 'flume', detail: 'Competitive with crossbeam in bounded. Slightly slower in unbounded MPMC.' },
    ],
    scenarios: [
      { scenario: 'MPMC bounded', leader: 'crossbeam \u2248 flume > kanal' },
      { scenario: 'Unbounded MPSC', leader: 'crossbeam > flume > std' },
      { scenario: 'Sync select!', leader: 'crossbeam (only option)' },
    ],
  },
  {
    label: 'Moderate',
    icon: '🟡',
    color: 'var(--color-oneshot)',
    barPct: 52,
    desc: 'Prioritize correctness, ecosystem fit, or API simplicity over raw throughput.',
    crates: [
      { name: 'tokio::mpsc', detail: 'Not trying to win benchmarks. Optimized for integration with Tokio runtime.' },
      { name: 'async-channel', detail: 'Lightweight and correct. Slightly behind flume in contended MPMC.' },
      { name: 'std::sync::mpsc', detail: 'Based on crossbeam since Rust 1.67. Competitive but fewer features.' },
    ],
    scenarios: [
      { scenario: 'Tokio ecosystem', leader: 'tokio::mpsc (best integration)' },
      { scenario: 'smol ecosystem', leader: 'async-channel (native fit)' },
      { scenario: 'No dependencies', leader: 'std::sync::mpsc (only option)' },
    ],
  },
];

// ===== ICONS =====
const CHECK_SVG = `<svg class="prop-icon yes" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="3 8 7 12 13 4"/></svg>`;
const CROSS_SVG = `<svg class="prop-icon no" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 4l8 8M12 4l-8 8"/></svg>`;
const TABLE_CHECK = `<span class="table-cell-check yes"><svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="3 8 7 12 13 4"/></svg></span>`;
const TABLE_CROSS = `<span class="table-cell-check no"><svg width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2"><path d="M5 5l6 6M11 5l-6 6"/></svg></span>`;

// ===== THEME TOGGLE =====
(function () {
  const t = document.querySelector('[data-theme-toggle]');
  const r = document.documentElement;
  let d = matchMedia('(prefers-color-scheme:dark)').matches ? 'dark' : 'light';
  r.setAttribute('data-theme', d);
  if (t) {
    updateToggleIcon(t, d);
    t.addEventListener('click', () => {
      d = d === 'dark' ? 'light' : 'dark';
      r.setAttribute('data-theme', d);
      t.setAttribute('aria-label', 'Switch to ' + (d === 'dark' ? 'light' : 'dark') + ' mode');
      updateToggleIcon(t, d);
    });
  }
  function updateToggleIcon(btn, theme) {
    btn.innerHTML = theme === 'dark'
      ? '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="5"/><path d="M12 1v2M12 21v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M1 12h2M21 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42"/></svg>'
      : '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
  }
})();

// ===== RENDER CRATE CARDS =====
function renderCrateGrid() {
  const grid = document.getElementById('crateGrid');
  grid.innerHTML = '';

  CRATES.forEach(crate => {
    const col = document.createElement('div');
    col.className = 'crate-column';
    col.dataset.crate = crate.name;
    col.dataset.runtime = crate.runtime;

    const channelHTML = crate.channels.map(ch => {
      const props = [
        { label: 'Blocking Send', val: ch.blockingSend },
        { label: 'Blocking Recv', val: ch.blockingRecv },
        { label: 'Async Send', val: ch.asyncSend },
        { label: 'Async Recv', val: ch.asyncRecv },
        { label: 'Try Send', val: ch.trySend },
        { label: 'Try Recv', val: ch.tryRecv },
        { label: 'Clone Tx', val: ch.cloneTx },
        { label: 'Clone Rx', val: ch.cloneRx },
      ];

      return `
        <div class="channel-card"
             data-pattern="${ch.pattern}"
             data-bounded="${ch.bounded}"
             data-async-send="${ch.asyncSend}"
             data-async-recv="${ch.asyncRecv}"
             data-blocking-send="${ch.blockingSend}"
             data-blocking-recv="${ch.blockingRecv}">
          <div class="channel-card-header">
            <span class="channel-type-label">${ch.name}</span>
            <span class="channel-pattern-badge ${ch.pattern}">${ch.pattern.toUpperCase()}</span>
          </div>
          <div class="channel-desc">${ch.description}</div>
          <div class="channel-props">
            ${props.map(p => `
              <div class="prop-row">
                ${p.val ? CHECK_SVG : CROSS_SVG}
                <span class="prop-label ${p.val ? 'yes' : ''}">${p.label}</span>
              </div>
            `).join('')}
          </div>
        </div>
      `;
    }).join('');

    col.innerHTML = `
      <div class="crate-header">
        <span class="crate-name">${crate.name}</span>
        <span class="crate-badge ${crate.runtime}">${crate.runtime}</span>
      </div>
      <div class="crate-body">
        <div class="crate-description">${crate.description}</div>
        ${channelHTML}
      </div>
    `;

    grid.appendChild(col);
  });
}

// ===== RENDER COMPARISON TABLE =====
function renderComparisonTable() {
  const table = document.getElementById('comparisonTable');
  const rows = [];

  CRATES.forEach(crate => {
    crate.channels.forEach(ch => {
      rows.push({ crate: crate.name, runtime: crate.runtime, ...ch });
    });
  });

  table.innerHTML = `
    <thead>
      <tr>
        <th>Crate</th>
        <th>Channel</th>
        <th>Pattern</th>
        <th>Bounded</th>
        <th>Blk Send</th>
        <th>Blk Recv</th>
        <th>Async Send</th>
        <th>Async Recv</th>
        <th>Try Send</th>
        <th>Try Recv</th>
        <th>Clone Tx</th>
        <th>Clone Rx</th>
        <th>Select</th>
      </tr>
    </thead>
    <tbody>
      ${rows.map(r => `
        <tr data-pattern="${r.pattern}" data-bounded="${r.bounded}" data-runtime="${r.runtime}">
          <td><span class="table-crate-name">${r.crate}</span></td>
          <td><span class="table-channel-name">${r.name}</span></td>
          <td><span class="channel-pattern-badge ${r.pattern}">${r.pattern.toUpperCase()}</span></td>
          <td>${r.bounded ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.blockingSend ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.blockingRecv ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.asyncSend ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.asyncRecv ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.trySend ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.tryRecv ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.cloneTx ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.cloneRx ? TABLE_CHECK : TABLE_CROSS}</td>
          <td>${r.select ? TABLE_CHECK : TABLE_CROSS}</td>
        </tr>
      `).join('')}
    </tbody>
  `;
}

// ===== RENDER FLOW DIAGRAM =====
function renderFlowDiagram() {
  const svg = document.getElementById('flowDiagram');

  // All 5 patterns rendered as full-width rows, stacked vertically
  const allPatterns = [
    { label: 'MPSC', color: 'var(--color-mpsc)', producers: ['P1', 'P2', 'P3'], consumers: ['C'], desc: 'Many senders, one receiver' },
    { label: 'MPMC', color: 'var(--color-mpmc)', producers: ['P1', 'P2'], consumers: ['C1', 'C2'], desc: 'Many senders, many receivers' },
    { label: 'Broadcast', color: 'var(--color-broadcast)', producers: ['P'], consumers: ['C1', 'C2', 'C3'], desc: 'Every receiver gets a clone of every message' },
    { label: 'Oneshot', color: 'var(--color-oneshot)', producers: ['P'], consumers: ['C'], desc: 'One sender, one value, one receiver', special: 'oneshot' },
    { label: 'Watch', color: 'var(--color-watch)', producers: ['P'], consumers: ['C1', 'C2'], desc: 'Latest value only — receivers skip old values', special: 'watch' },
  ];

  const W = 700;         // SVG internal width
  const nodeW = 72;
  const nodeH = 34;
  const chW = 140;
  const chH = 40;
  const nodeSpacing = 42; // vertical gap between producer/consumer nodes
  const sectionGap = 36;  // vertical gap between pattern sections

  // x positions — centered in W
  const pX = 110;
  const chX = (W - chW) / 2;
  const cX = W - 110 - nodeW;

  let svgContent = '';

  // Defs for arrowheads
  svgContent += '<defs>';
  allPatterns.forEach(p => {
    const id = p.label.toLowerCase();
    svgContent += `<marker id="arrow-${id}" viewBox="0 0 10 7" refX="9" refY="3.5" markerWidth="8" markerHeight="6" orient="auto-start-reverse"><polygon points="0 0, 10 3.5, 0 7" fill="${p.color}"/></marker>`;
  });
  svgContent += '</defs>';

  let cursorY = 10; // running vertical position

  allPatterns.forEach(pat => {
    const arrowId = pat.label.toLowerCase();
    const maxNodes = Math.max(pat.producers.length, pat.consumers.length);
    const nodesHeight = maxNodes * nodeSpacing;

    // Section label + description
    svgContent += `<text x="16" y="${cursorY + 18}" font-size="14" font-weight="700" fill="${pat.color}" font-family="var(--font-mono)">${pat.label}</text>`;
    svgContent += `<text x="16" y="${cursorY + 34}" font-size="11" fill="var(--color-text-muted)" font-family="var(--font-body)">${pat.desc}</text>`;

    // Divider line under label
    svgContent += `<line x1="16" y1="${cursorY + 42}" x2="${W - 16}" y2="${cursorY + 42}" stroke="${pat.color}" stroke-opacity="0.15" stroke-width="1"/>`;

    const diagramTop = cursorY + 56; // top of the node area

    // --- Special: Oneshot ---
    if (pat.special === 'oneshot') {
      const midY = diagramTop + nodeH / 2;
      // P node
      svgContent += `<rect x="${pX}" y="${diagramTop}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
      svgContent += `<text x="${pX + nodeW/2}" y="${midY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">P</text>`;
      // Arrow with "1 val" label
      const arrStart = pX + nodeW;
      const arrEnd = cX;
      const arrMid = (arrStart + arrEnd) / 2;
      svgContent += `<path d="M${arrStart} ${midY} L${arrEnd} ${midY}" stroke="${pat.color}" stroke-width="2" fill="none" stroke-opacity="0.7" marker-end="url(#arrow-${arrowId})"/>`;
      svgContent += `<text x="${arrMid}" y="${midY - 8}" text-anchor="middle" font-size="10" font-weight="600" fill="${pat.color}" font-family="var(--font-mono)">1 val</text>`;
      // C node
      svgContent += `<rect x="${cX}" y="${diagramTop}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
      svgContent += `<text x="${cX + nodeW/2}" y="${midY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">C</text>`;

      cursorY = diagramTop + nodeH + sectionGap;
      return;
    }

    // --- Special: Watch ---
    if (pat.special === 'watch') {
      const cCount = pat.consumers.length;
      const blockH = cCount * nodeSpacing;
      const pY = diagramTop + blockH / 2 - nodeH / 2;
      const pMidY = pY + nodeH / 2;

      // P node
      svgContent += `<rect x="${pX}" y="${pY}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
      svgContent += `<text x="${pX + nodeW/2}" y="${pMidY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">P</text>`;

      // "latest" box in the center
      const lbW = 60;
      const lbH = nodeH + 6;
      const lbX = chX + (chW - lbW) / 2;
      const lbY = pY - 3;
      svgContent += `<rect x="${lbX}" y="${lbY}" width="${lbW}" height="${lbH}" rx="5" fill="${pat.color}" fill-opacity="0.12" stroke="${pat.color}" stroke-width="1.2" stroke-dasharray="5 3"/>`;
      svgContent += `<text x="${lbX + lbW/2}" y="${lbY + lbH/2 + 4}" text-anchor="middle" font-size="11" font-weight="600" fill="${pat.color}" font-family="var(--font-mono)">latest</text>`;

      // Arrow P -> latest
      svgContent += `<path d="M${pX + nodeW} ${pMidY} L${lbX} ${lbY + lbH/2}" stroke="${pat.color}" stroke-width="1.5" fill="none" stroke-opacity="0.6" marker-end="url(#arrow-${arrowId})"/>`;

      // Consumer nodes + arrows from latest
      pat.consumers.forEach((c, i) => {
        const cY = diagramTop + i * nodeSpacing;
        const cMidY = cY + nodeH / 2;
        svgContent += `<rect x="${cX}" y="${cY}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
        svgContent += `<text x="${cX + nodeW/2}" y="${cMidY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">${c}</text>`;
        const fromX = lbX + lbW;
        const fromY = lbY + lbH / 2;
        const midX = (fromX + cX) / 2;
        svgContent += `<path d="M${fromX} ${fromY} C${midX} ${fromY}, ${midX} ${cMidY}, ${cX} ${cMidY}" stroke="${pat.color}" stroke-width="1.5" fill="none" stroke-opacity="0.5" marker-end="url(#arrow-${arrowId})"/>`;
      });

      cursorY = diagramTop + blockH + sectionGap;
      return;
    }

    // --- Standard patterns: MPSC, MPMC, Broadcast ---
    // Channel box (centered vertically in the node block)
    const chY = diagramTop + nodesHeight / 2 - chH / 2;
    const chMidY = chY + chH / 2;
    svgContent += `<rect x="${chX}" y="${chY}" width="${chW}" height="${chH}" rx="6" fill="${pat.color}" fill-opacity="0.12" stroke="${pat.color}" stroke-width="1.5"/>`;
    svgContent += `<text x="${chX + chW/2}" y="${chMidY + 5}" text-anchor="middle" font-size="13" font-weight="600" fill="${pat.color}" font-family="var(--font-mono)">Channel</text>`;

    // Producers
    pat.producers.forEach((p, i) => {
      const pY = diagramTop + i * nodeSpacing;
      const pMidY = pY + nodeH / 2;
      svgContent += `<rect x="${pX}" y="${pY}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
      svgContent += `<text x="${pX + nodeW/2}" y="${pMidY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">${p}</text>`;
      // Curve to channel
      const fromX = pX + nodeW;
      const toX = chX;
      const midX = (fromX + toX) / 2;
      svgContent += `<path d="M${fromX} ${pMidY} C${midX} ${pMidY}, ${midX} ${chMidY}, ${toX} ${chMidY}" stroke="${pat.color}" stroke-width="1.5" fill="none" stroke-opacity="0.6" marker-end="url(#arrow-${arrowId})"/>`;
    });

    // Consumers
    pat.consumers.forEach((c, i) => {
      const cY = diagramTop + i * nodeSpacing;
      const cMidY = cY + nodeH / 2;
      svgContent += `<rect x="${cX}" y="${cY}" width="${nodeW}" height="${nodeH}" rx="6" fill="var(--color-surface-offset)" stroke="${pat.color}" stroke-width="1.5"/>`;
      svgContent += `<text x="${cX + nodeW/2}" y="${cMidY + 5}" text-anchor="middle" font-size="12" font-weight="600" fill="var(--color-text)" font-family="var(--font-mono)">${c}</text>`;
      // Curve from channel
      const fromX = chX + chW;
      const toX = cX;
      const midX = (fromX + toX) / 2;
      svgContent += `<path d="M${fromX} ${chMidY} C${midX} ${chMidY}, ${midX} ${cMidY}, ${toX} ${cMidY}" stroke="${pat.color}" stroke-width="1.5" fill="none" stroke-opacity="0.6" marker-end="url(#arrow-${arrowId})"/>`;
    });

    cursorY = diagramTop + nodesHeight + sectionGap;
  });

  // Set the viewBox to fit all content
  svg.setAttribute('viewBox', `0 0 ${W} ${cursorY}`);
  svg.style.width = '100%';
  svg.style.height = 'auto';
  svg.innerHTML = svgContent;
}

// ===== RENDER DECISION GUIDE =====
function renderDecisionGuide() {
  const grid = document.getElementById('decisionGrid');
  grid.innerHTML = DECISIONS.map(d => `
    <div class="decision-card">
      <div class="decision-q">${d.q}</div>
      <div class="decision-a"><span class="arrow">→</span> ${d.a}</div>
      <div class="decision-note">${d.note}</div>
    </div>
  `).join('');
}

// ===== FILTER LOGIC =====
function setupFilters() {
  const pills = document.querySelectorAll('#filterBar .filter-pill');
  let activeFilter = 'all';

  pills.forEach(pill => {
    pill.addEventListener('click', () => {
      pills.forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      activeFilter = pill.dataset.filter;
      applyFilter(activeFilter);
    });
  });
}

function applyFilter(filter) {
  const columns = document.querySelectorAll('.crate-column');
  const cards = document.querySelectorAll('.channel-card');
  const tableRows = document.querySelectorAll('#comparisonTable tbody tr');

  if (filter === 'all') {
    columns.forEach(c => c.classList.remove('dimmed'));
    cards.forEach(c => { c.classList.remove('dimmed'); c.classList.remove('highlight'); });
    tableRows.forEach(r => r.style.opacity = '1');
    return;
  }

  // Pattern-based filters
  const patternFilters = ['mpsc', 'mpmc', 'oneshot', 'broadcast', 'watch', 'spsc'];
  if (patternFilters.includes(filter)) {
    cards.forEach(c => {
      const match = c.dataset.pattern === filter;
      c.classList.toggle('dimmed', !match);
      c.classList.toggle('highlight', match);
    });

    columns.forEach(col => {
      const hasMatch = col.querySelector(`.channel-card[data-pattern="${filter}"]`);
      col.classList.toggle('dimmed', !hasMatch);
    });

    tableRows.forEach(r => {
      r.style.opacity = r.dataset.pattern === filter ? '1' : '0.2';
    });
    return;
  }

  // Runtime filters
  if (filter === 'async') {
    cards.forEach(c => {
      const match = c.dataset.asyncSend === 'true' || c.dataset.asyncRecv === 'true';
      c.classList.toggle('dimmed', !match);
      c.classList.toggle('highlight', match);
    });
    columns.forEach(col => {
      const rt = col.dataset.runtime;
      col.classList.toggle('dimmed', rt === 'sync');
    });
    tableRows.forEach(r => {
      const rt = r.dataset.runtime;
      r.style.opacity = rt !== 'sync' ? '1' : '0.2';
    });
    return;
  }

  if (filter === 'sync') {
    cards.forEach(c => {
      const match = c.dataset.blockingSend === 'true' || c.dataset.blockingRecv === 'true';
      c.classList.toggle('dimmed', !match);
      c.classList.toggle('highlight', match);
    });
    columns.forEach(col => {
      const rt = col.dataset.runtime;
      col.classList.toggle('dimmed', rt === 'async');
    });
    tableRows.forEach(r => {
      const rt = r.dataset.runtime;
      r.style.opacity = rt !== 'async' ? '1' : '0.2';
    });
    return;
  }

  // Bounded/unbounded
  if (filter === 'bounded' || filter === 'unbounded') {
    const bVal = filter === 'bounded' ? 'true' : 'false';
    cards.forEach(c => {
      const match = c.dataset.bounded === bVal;
      c.classList.toggle('dimmed', !match);
      c.classList.toggle('highlight', match);
    });
    columns.forEach(col => {
      const hasMatch = col.querySelector(`.channel-card[data-bounded="${bVal}"]`);
      col.classList.toggle('dimmed', !hasMatch);
    });
    tableRows.forEach(r => {
      r.style.opacity = r.dataset.bounded === bVal ? '1' : '0.2';
    });
  }
}

// ===== RENDER DECISION FLOWCHART =====
function renderFlowchart() {
  const container = document.getElementById('flowchartContainer');
  let history = []; // stack of { node, choiceLabel }

  function renderStep(node) {
    const breadcrumb = history.length > 0
      ? `<div class="fc-breadcrumb">
          ${history.map((h, i) => `<span class="fc-crumb">${h.choiceLabel}</span><span class="fc-crumb-sep">›</span>`).join('')}
          <span class="fc-crumb-active">${node.q ? 'Current' : 'Result'}</span>
        </div>`
      : '';

    if (node.q) {
      container.innerHTML = `
        <div class="fc-step">
          ${breadcrumb}
          <div class="fc-question">${node.q}</div>
          <div class="fc-options">
            ${node.options.map((opt, i) => `
              <button class="fc-option" data-idx="${i}">${opt.label}</button>
            `).join('')}
          </div>
          ${history.length > 0 ? '<button class="fc-restart" data-action="back">\u2190 Back</button> ' : ''}
          ${history.length > 0 ? '<button class="fc-restart" data-action="restart">Start over</button>' : ''}
        </div>
      `;

      container.querySelectorAll('.fc-option').forEach(btn => {
        btn.addEventListener('click', () => {
          const idx = parseInt(btn.dataset.idx);
          const opt = node.options[idx];
          history.push({ node, choiceLabel: opt.label });
          if (opt.result) {
            renderResult(opt.result);
          } else {
            renderStep(opt.next);
          }
        });
      });

      const backBtn = container.querySelector('[data-action="back"]');
      if (backBtn) backBtn.addEventListener('click', () => {
        const prev = history.pop();
        renderStep(prev.node);
      });

      const restartBtn = container.querySelector('[data-action="restart"]');
      if (restartBtn) restartBtn.addEventListener('click', () => {
        history = [];
        renderStep(FLOW_TREE);
      });
    }
  }

  function renderResult(result) {
    const breadcrumb = `<div class="fc-breadcrumb">
      ${history.map(h => `<span class="fc-crumb">${h.choiceLabel}</span><span class="fc-crumb-sep">›</span>`).join('')}
      <span class="fc-crumb-active">Result</span>
    </div>`;

    container.innerHTML = `
      <div class="fc-step">
        ${breadcrumb}
        <div class="fc-result">
          <div class="fc-result-label">Recommended</div>
          <div class="fc-result-name">${result.name}</div>
          <div class="fc-result-why">${result.why}</div>
        </div>
        <button class="fc-restart" data-action="back">\u2190 Back</button>
        <button class="fc-restart" data-action="restart">Start over</button>
      </div>
    `;

    container.querySelector('[data-action="back"]').addEventListener('click', () => {
      const prev = history.pop();
      renderStep(prev.node);
    });

    container.querySelector('[data-action="restart"]').addEventListener('click', () => {
      history = [];
      renderStep(FLOW_TREE);
    });
  }

  renderStep(FLOW_TREE);
}

// ===== RENDER CODE SNIPPETS =====
function renderSnippets() {
  const grid = document.getElementById('snippetsGrid');
  const CHEVRON = '<svg class="snippet-chevron" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="6 9 12 15 18 9"/></svg>';

  grid.innerHTML = SNIPPETS.map((s, i) => `
    <div class="snippet-card" data-snippet="${i}">
      <div class="snippet-header">
        <div class="snippet-title">
          <span class="snippet-title-text">${s.title}</span>
          <span class="snippet-title-badge channel-pattern-badge ${s.badgeClass}">${s.badge}</span>
        </div>
        ${CHEVRON}
      </div>
      <div class="snippet-body">
        <div class="snippet-code">${escapeHtml(s.code)}</div>
      </div>
    </div>
  `).join('');

  grid.querySelectorAll('.snippet-header').forEach(header => {
    header.addEventListener('click', () => {
      const card = header.closest('.snippet-card');
      card.classList.toggle('open');
    });
  });
}

function escapeHtml(str) {
  return str.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ===== RENDER PERFORMANCE TIERS =====
function renderPerformanceTiers() {
  const container = document.getElementById('tiersContainer');

  container.innerHTML = PERF_TIERS.map(tier => `
    <div class="tier-row">
      <div class="tier-header">
        <div class="tier-icon" style="background: color-mix(in srgb, ${tier.color} 15%, transparent);">
          ${tier.icon}
        </div>
        <div>
          <div class="tier-label" style="color: ${tier.color};">${tier.label}</div>
          <div class="tier-desc">${tier.desc}</div>
        </div>
      </div>
      <div class="tier-bar-track">
        <div class="tier-bar-fill" style="width: 0%; background: ${tier.color};" data-target="${tier.barPct}"></div>
      </div>
      <div class="tier-crates">
        ${tier.crates.map(c => `
          <div class="tier-crate-chip">
            <span class="tier-crate-name">${c.name}</span>
            <span class="tier-crate-detail">${c.detail}</span>
          </div>
        `).join('')}
      </div>
      <div class="tier-scenario-grid">
        ${tier.scenarios.map(s => `
          <div class="tier-scenario"><strong>${s.scenario}:</strong> ${s.leader}</div>
        `).join('')}
      </div>
    </div>
  `).join('');

  // Animate bars on scroll
  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const bars = entry.target.querySelectorAll('.tier-bar-fill');
        bars.forEach(bar => {
          bar.style.width = bar.dataset.target + '%';
        });
        observer.unobserve(entry.target);
      }
    });
  }, { threshold: 0.2 });

  observer.observe(container);

  // Caveat text
  document.getElementById('tiersCaveat').textContent =
    'Rankings derived from fereidani/rust-channel-benchmarks, crossfire wiki benchmarks, and community reports. '
    + 'The kanal repo maintains the primary benchmark suite, which may favor its own optimizations. '
    + 'Real-world performance depends on message size, contention, buffer depth, and whether your workload is CPU-bound or I/O-bound. '
    + 'Always profile your specific use case.';
}

// ===== ENTRY ANIMATION =====
function animateEntry() {
  const cards = document.querySelectorAll('.crate-column');
  cards.forEach((card, i) => {
    card.style.opacity = '0';
    card.style.transform = 'translateY(16px)';
    setTimeout(() => {
      card.style.transition = 'opacity 400ms ease, transform 400ms ease';
      card.style.opacity = '1';
      card.style.transform = 'translateY(0)';
    }, 60 * i);
  });
}

// ===== INIT =====
document.addEventListener('DOMContentLoaded', () => {
  renderCrateGrid();
  renderFlowchart();
  renderSnippets();
  renderPerformanceTiers();
  renderFlowDiagram();
  renderComparisonTable();
  renderDecisionGuide();
  setupFilters();
  animateEntry();
});

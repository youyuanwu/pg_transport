//! `bench` — latency + throughput harness for pg_transport.
//!
//! Phase 5 deliverable per
//! [`docs/design/roadmap.md` §1](../../docs/design/roadmap.md):
//! reports p50/p95/p99 latency and throughput; produces a CSV per
//! run.
//!
//! Drives [`tokio_postgres`] as the client (libpq-compatible
//! protocol; same wire bytes psql sends). For every target it
//! does:
//!
//! 1. Connect.
//! 2. Run `warmup` simple-query round-trips of `SELECT 1` to prime
//!    the slot, the wire driver, the SPI plan cache, and the CPU's
//!    branch predictors.
//! 3. Run `iterations` measured round-trips, capturing per-query
//!    wall time.
//! 4. Sort, dump min/p50/p95/p99/max + mean latency + throughput.
//!
//! With the `compare` subcommand, runs the same workload against
//! both the pg_transport tcp_handoff listener and the cluster's
//! vanilla PG listener, side-by-side. This is the entire point of
//! the bench harness: without a baseline, "we're at X µs" is
//! meaningless.
//!
//! CLI:
//!
//! ```text
//! bench --target HOST:PORT [--iterations N] [--warmup N]
//!       [--connections N] [--user USER] [--db DB] [--csv FILE]
//!       [--label LABEL]
//! bench compare [--iterations N] [--warmup N] [--connections N]
//!       [--pt-port P] [--pg-port P] [--user USER] [--db DB]
//!       [--csv FILE]
//! ```
//!
//! Defaults: iterations=1000 (TOTAL across all workers), warmup=100
//! (per worker), connections=1, user=postgres, db=postgres,
//! host=127.0.0.1, pt-port=5454, pg-port=54329.
//!
//! Phase ≥ 6 will grow this: multiple statement shapes, baseline
//! matrix (TCP/UDS, cold/warm slot), histogram CDF dump.

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::Barrier;
use tokio_postgres::NoTls;

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = env::args();
    let prog = args.next().unwrap_or_else(|| "bench".to_string());
    let args: Vec<String> = args.collect();

    if args.is_empty() {
        print_usage(&prog);
        std::process::exit(2);
    }

    if args[0] == "compare" {
        run_compare(&args[1..]).await
    } else {
        run_single(&args).await
    }
}

// ---------------------------------------------------------------------------
// Single target
// ---------------------------------------------------------------------------

async fn run_single(args: &[String]) -> Result<()> {
    let mut cfg = SingleArgs::default();
    cfg.parse(args)?;
    let conn_str = cfg.conn_string();

    eprintln!(
        "bench: target={} iterations={} warmup={} connections={}",
        cfg.target, cfg.iterations, cfg.warmup, cfg.connections
    );

    let (samples, wall) =
        run_workload(&conn_str, cfg.iterations, cfg.warmup, cfg.connections).await?;
    let summary = summarize(&samples, wall);
    let label = cfg.label.clone().unwrap_or_else(|| cfg.target.clone());
    print_summary(&label, cfg.connections, &summary);

    if let Some(path) = &cfg.csv {
        write_csv(path, &[(label, samples.clone())])?;
        eprintln!("bench: wrote {} samples to {}", samples.len(), path);
    }

    Ok(())
}

#[derive(Debug)]
struct SingleArgs {
    target: String,
    user: String,
    db: String,
    iterations: usize,
    warmup: usize,
    connections: usize,
    csv: Option<String>,
    label: Option<String>,
}

impl Default for SingleArgs {
    fn default() -> Self {
        Self {
            target: "127.0.0.1:5454".to_string(),
            user: "postgres".to_string(),
            db: "postgres".to_string(),
            iterations: 1000,
            warmup: 100,
            connections: 1,
            csv: None,
            label: None,
        }
    }
}

impl SingleArgs {
    fn parse(&mut self, args: &[String]) -> Result<()> {
        let mut i = 0;
        while i < args.len() {
            let arg = &args[i];
            let val = || -> Result<&String> {
                args.get(i + 1)
                    .ok_or_else(|| anyhow!("flag {arg} requires a value"))
            };
            match arg.as_str() {
                "--target" => {
                    self.target = val()?.clone();
                    i += 2;
                }
                "--user" => {
                    self.user = val()?.clone();
                    i += 2;
                }
                "--db" => {
                    self.db = val()?.clone();
                    i += 2;
                }
                "--iterations" => {
                    self.iterations = val()?
                        .parse()
                        .context("--iterations must be a positive integer")?;
                    i += 2;
                }
                "--warmup" => {
                    self.warmup = val()?
                        .parse()
                        .context("--warmup must be a non-negative integer")?;
                    i += 2;
                }
                "--connections" => {
                    self.connections = val()?
                        .parse()
                        .context("--connections must be a positive integer")?;
                    i += 2;
                }
                "--csv" => {
                    self.csv = Some(val()?.clone());
                    i += 2;
                }
                "--label" => {
                    self.label = Some(val()?.clone());
                    i += 2;
                }
                "-h" | "--help" => {
                    print_usage("bench");
                    std::process::exit(0);
                }
                other => bail!("unknown flag {other:?}"),
            }
        }
        if self.iterations == 0 {
            bail!("--iterations must be > 0");
        }
        if self.connections == 0 {
            bail!("--connections must be > 0");
        }
        if self.connections > self.iterations {
            bail!(
                "--connections ({}) > --iterations ({}); each worker needs at least one query",
                self.connections,
                self.iterations
            );
        }
        Ok(())
    }

    fn conn_string(&self) -> String {
        // Tokio-postgres connect-string format:
        // `host=H port=P user=U dbname=D` (libpq-style).
        let (host, port) = split_target(&self.target);
        format!(
            "host={host} port={port} user={u} dbname={d}",
            u = self.user,
            d = self.db
        )
    }
}

// ---------------------------------------------------------------------------
// Compare
// ---------------------------------------------------------------------------

async fn run_compare(args: &[String]) -> Result<()> {
    let mut cfg = CompareArgs::default();
    cfg.parse(args)?;

    eprintln!(
        "bench compare: pt={} pg={} iterations={} warmup={} connections={}",
        cfg.pt_target(),
        cfg.pg_target(),
        cfg.iterations,
        cfg.warmup,
        cfg.connections,
    );

    let pt_conn = format!(
        "host=127.0.0.1 port={} user={} dbname={}",
        cfg.pt_port, cfg.user, cfg.db
    );
    let pg_conn = format!(
        "host=127.0.0.1 port={} user={} dbname={}",
        cfg.pg_port, cfg.user, cfg.db
    );

    // Run vanilla first as the baseline reference, then pg_transport.
    // Sequential (not parallel) so the two share no contention; the
    // numbers should be a clean delta.
    let (pg_samples, pg_wall) = run_workload(&pg_conn, cfg.iterations, cfg.warmup, cfg.connections)
        .await
        .context("vanilla PG bench")?;
    let (pt_samples, pt_wall) = run_workload(&pt_conn, cfg.iterations, cfg.warmup, cfg.connections)
        .await
        .context("pg_transport bench")?;

    let pg_sum = summarize(&pg_samples, pg_wall);
    let pt_sum = summarize(&pt_samples, pt_wall);

    print_compare(cfg.connections, &pg_sum, &pt_sum);

    if let Some(path) = &cfg.csv {
        write_csv(
            path,
            &[
                ("vanilla".to_string(), pg_samples),
                ("pg_transport".to_string(), pt_samples),
            ],
        )?;
        eprintln!(
            "bench: wrote {} samples per target to {path}",
            cfg.iterations
        );
    }

    Ok(())
}

#[derive(Debug)]
struct CompareArgs {
    pt_port: u16,
    pg_port: u16,
    user: String,
    db: String,
    iterations: usize,
    warmup: usize,
    connections: usize,
    csv: Option<String>,
}

impl Default for CompareArgs {
    fn default() -> Self {
        Self {
            pt_port: 5454,
            pg_port: 54329,
            user: "postgres".to_string(),
            db: "postgres".to_string(),
            iterations: 1000,
            warmup: 100,
            connections: 1,
            csv: None,
        }
    }
}

impl CompareArgs {
    fn parse(&mut self, args: &[String]) -> Result<()> {
        let mut i = 0;
        while i < args.len() {
            let arg = &args[i];
            let val = || -> Result<&String> {
                args.get(i + 1)
                    .ok_or_else(|| anyhow!("flag {arg} requires a value"))
            };
            match arg.as_str() {
                "--pt-port" => {
                    self.pt_port = val()?.parse().context("--pt-port must be a u16")?;
                    i += 2;
                }
                "--pg-port" => {
                    self.pg_port = val()?.parse().context("--pg-port must be a u16")?;
                    i += 2;
                }
                "--user" => {
                    self.user = val()?.clone();
                    i += 2;
                }
                "--db" => {
                    self.db = val()?.clone();
                    i += 2;
                }
                "--iterations" => {
                    self.iterations = val()?.parse().context("--iterations")?;
                    i += 2;
                }
                "--warmup" => {
                    self.warmup = val()?.parse().context("--warmup")?;
                    i += 2;
                }
                "--connections" => {
                    self.connections = val()?.parse().context("--connections")?;
                    i += 2;
                }
                "--csv" => {
                    self.csv = Some(val()?.clone());
                    i += 2;
                }
                "-h" | "--help" => {
                    print_usage("bench");
                    std::process::exit(0);
                }
                other => bail!("unknown compare flag {other:?}"),
            }
        }
        if self.iterations == 0 {
            bail!("--iterations must be > 0");
        }
        if self.connections == 0 {
            bail!("--connections must be > 0");
        }
        if self.connections > self.iterations {
            bail!(
                "--connections ({}) > --iterations ({}); each worker needs at least one query",
                self.connections,
                self.iterations
            );
        }
        Ok(())
    }

    fn pt_target(&self) -> String {
        format!("pg_transport@127.0.0.1:{}", self.pt_port)
    }
    fn pg_target(&self) -> String {
        format!("vanilla@127.0.0.1:{}", self.pg_port)
    }
}

// ---------------------------------------------------------------------------
// Workload + summary
// ---------------------------------------------------------------------------

/// Top-level workload runner: spawn `connections` workers, each
/// with its own tokio-postgres connection. Each worker connects,
/// runs `warmup` SELECT 1 round-trips to prime its slot + plan
/// cache, then BARRIERS with the others. Wall clock starts on
/// barrier release, ends when every worker has finished its share
/// of `iterations` measured round-trips. Returns (all per-query
/// latencies, wall clock of the measured phase).
///
/// QPS is computed by the caller as `iterations / wall_clock` — NOT
/// `iterations / sum(samples)` (which over-counts under concurrency,
/// since per-worker samples overlap in real time).
///
/// Barrier timeout: with v0's one-connection-per-slot model
/// ([Q10 in roadmap.md](../../docs/design/roadmap.md) deferred),
/// `connections > backend_pool_size` will deadlock — extra
/// connections queue on a busy slot's UDS and can't even complete
/// their TCP startup, so they never reach the barrier. We time the
/// barrier wait out at 30 s and surface the misconfig as a
/// readable error rather than hanging.
const BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

async fn run_workload(
    conn_str: &str,
    iterations: usize,
    warmup: usize,
    connections: usize,
) -> Result<(Vec<Duration>, Duration)> {
    let per_worker_base = iterations / connections;
    let extra = iterations % connections;
    // `+1` accounts for the coordinator that releases the barrier
    // and then awaits worker completion.
    let barrier = Arc::new(Barrier::new(connections + 1));

    let mut handles = Vec::with_capacity(connections);
    for worker_id in 0..connections {
        let iters = per_worker_base + if worker_id < extra { 1 } else { 0 };
        let conn_str = conn_str.to_string();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            run_one_worker(&conn_str, iters, warmup, barrier).await
        }));
    }

    // Wait for every worker to reach the barrier (i.e. finish its
    // warmup and be ready to measure). Time out if they don't —
    // see BARRIER_TIMEOUT above.
    match tokio::time::timeout(BARRIER_TIMEOUT, barrier.wait()).await {
        Ok(_) => {}
        Err(_) => {
            // Don't abort the workers explicitly; let them stay
            // pending so any in-flight queries don't leak. The
            // process exit on Err return takes them with it.
            bail!(
                "barrier timeout after {:?}: only some workers finished warmup. \
                 If `--connections` ({}) > pg_transport.backend_pool_size, the \
                 extra connections queue on a busy slot's UDS and can never \
                 complete startup (v0 limitation; see Q10 in \
                 docs/design/roadmap.md).",
                BARRIER_TIMEOUT,
                connections
            );
        }
    }
    let wall_start = Instant::now();

    let mut all_samples = Vec::with_capacity(iterations);
    let mut first_err: Option<anyhow::Error> = None;
    for h in handles {
        match h.await {
            Ok(Ok(samples)) => all_samples.extend(samples),
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(join_err) => {
                if first_err.is_none() {
                    first_err = Some(anyhow::anyhow!("worker join: {join_err}"));
                }
            }
        }
    }
    let wall = wall_start.elapsed();

    if let Some(e) = first_err {
        return Err(e);
    }
    Ok((all_samples, wall))
}

/// One worker: connect, warmup, barrier, measured iters, teardown.
async fn run_one_worker(
    conn_str: &str,
    iterations: usize,
    warmup: usize,
    barrier: Arc<Barrier>,
) -> Result<Vec<Duration>> {
    let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .with_context(|| format!("connect: {conn_str}"))?;
    // The connection task pumps reads/writes; if it errors, the
    // client's calls below start failing — caller will see it.
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("bench: connection task error: {e}");
        }
    });

    // Warmup: not measured. We use `simple_query` (text protocol,
    // matching what our wire handles via SimpleQueryHandler) instead
    // of the extended-protocol `query` so we're benchmarking the
    // same path psql -c uses.
    for _ in 0..warmup {
        let _ = client
            .simple_query("SELECT 1")
            .await
            .context("warmup query")?;
    }

    // Wait for every other worker to also finish warmup.
    barrier.wait().await;

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        let _ = client
            .simple_query("SELECT 1")
            .await
            .context("measured query")?;
        samples.push(t.elapsed());
    }

    // Drop the client → connection task exits → join.
    drop(client);
    let _ = conn_handle.await;

    Ok(samples)
}

#[derive(Debug, Clone, Copy)]
struct Summary {
    n: usize,
    wall: Duration,
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
    qps: f64,
}

fn summarize(samples: &[Duration], wall: Duration) -> Summary {
    assert!(
        !samples.is_empty(),
        "summarize requires at least one sample"
    );
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort();
    let n = sorted.len();
    let mean = Duration::from_nanos(
        (samples.iter().map(|d| d.as_nanos()).sum::<u128>() / n as u128) as u64,
    );
    let pct = |p: usize| {
        // index of the percentile; nearest-rank, clamped to n-1.
        let idx = ((p * n) / 100).min(n - 1);
        sorted[idx]
    };
    Summary {
        n,
        wall,
        min: sorted[0],
        p50: pct(50),
        p95: pct(95),
        p99: pct(99),
        max: sorted[n - 1],
        mean,
        qps: n as f64 / wall.as_secs_f64(),
    }
}

fn fmt_us(d: Duration) -> String {
    // Microseconds with 1 decimal — convenient for the latency
    // numbers we're likely to see (single-digit to low-hundreds µs).
    format!("{:.1}", d.as_secs_f64() * 1_000_000.0)
}

fn print_summary(label: &str, connections: usize, s: &Summary) {
    eprintln!();
    eprintln!("--- {label} (connections={connections}) ---");
    eprintln!("n        : {}", s.n);
    eprintln!("wall     : {:?}", s.wall);
    eprintln!("qps      : {:>8.1}", s.qps);
    eprintln!("min      : {:>8} µs", fmt_us(s.min));
    eprintln!("p50      : {:>8} µs", fmt_us(s.p50));
    eprintln!("p95      : {:>8} µs", fmt_us(s.p95));
    eprintln!("p99      : {:>8} µs", fmt_us(s.p99));
    eprintln!("max      : {:>8} µs", fmt_us(s.max));
    eprintln!("mean     : {:>8} µs", fmt_us(s.mean));
}

fn print_compare(connections: usize, pg: &Summary, pt: &Summary) {
    let ratio = |a: Duration, b: Duration| {
        if b.as_nanos() == 0 {
            "n/a".to_string()
        } else {
            format!("{:.2}x", a.as_secs_f64() / b.as_secs_f64())
        }
    };
    eprintln!();
    eprintln!("connections={connections}");
    eprintln!(
        "{:<10}  {:>10}  {:>14}  {:>14}  {:>10}",
        "stat", "vanilla µs", "pg_transport µs", "diff µs", "pt/pg"
    );
    eprintln!(
        "{:-<10}  {:->10}  {:->14}  {:->14}  {:->10}",
        "", "", "", "", ""
    );
    let row = |label: &str, a: Duration, b: Duration| {
        let diff = if b > a { b - a } else { Duration::ZERO };
        eprintln!(
            "{:<10}  {:>10}  {:>14}  {:>14}  {:>10}",
            label,
            fmt_us(a),
            fmt_us(b),
            fmt_us(diff),
            ratio(b, a),
        );
    };
    row("min", pg.min, pt.min);
    row("p50", pg.p50, pt.p50);
    row("p95", pg.p95, pt.p95);
    row("p99", pg.p99, pt.p99);
    row("max", pg.max, pt.max);
    row("mean", pg.mean, pt.mean);
    eprintln!(
        "qps        {:>10.1}  {:>14.1}                  {:>10}",
        pg.qps,
        pt.qps,
        format!("{:.2}x", pt.qps / pg.qps),
    );
}

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

fn write_csv(path: &str, runs: &[(String, Vec<Duration>)]) -> Result<()> {
    let f = File::create(path).with_context(|| format!("creating {path}"))?;
    let mut w = BufWriter::new(f);
    writeln!(w, "label,iter,latency_ns")?;
    for (label, samples) in runs {
        for (i, d) in samples.iter().enumerate() {
            writeln!(w, "{label},{i},{}", d.as_nanos())?;
        }
    }
    w.flush()?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Misc
// ---------------------------------------------------------------------------

fn split_target(t: &str) -> (&str, &str) {
    match t.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => (t, "5454"),
    }
}

fn print_usage(prog: &str) {
    eprintln!(
        "\
bench — pg_transport latency / throughput harness.

USAGE:
    {prog} --target HOST:PORT [--iterations N] [--warmup N]
                [--connections N] [--user U] [--db D]
                [--csv FILE] [--label L]

    {prog} compare [--pt-port P] [--pg-port P] [--iterations N]
                [--warmup N] [--connections N]
                [--user U] [--db D] [--csv FILE]

Defaults: iterations=1000 (TOTAL across workers), warmup=100 (per
worker), connections=1, user=postgres, db=postgres, host=127.0.0.1,
pt-port=5454, pg-port=54329.

Examples:
    bench --target 127.0.0.1:5454 --iterations 5000
    bench compare --iterations 5000 --connections 4 --csv /tmp/b.csv
"
    );
}

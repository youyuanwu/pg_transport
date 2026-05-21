//! `bench` — latency + throughput harness for pg_transport.
//!
//! Phase-5 deliverable per
//! [`docs/design/roadmap.md` §1](../../docs/design/roadmap.md):
//! reports p50/p95/p99 latency and throughput; produces a CSV per
//! run.
//!
//! Drives [`tokio_postgres`] as the client (libpq-compatible
//! protocol; same wire bytes psql sends). For every target it:
//!
//! 1. Spawns `--connections` worker tasks, each on its own
//!    tokio-postgres connection.
//! 2. Each worker runs `--warmup` `SELECT 1` round-trips to prime
//!    its slot + plan cache, then waits on a barrier.
//! 3. Barrier release starts the wall clock. Every worker runs
//!    its share of `--iterations` measured round-trips.
//! 4. Aggregate latencies → min / p50 / p95 / p99 / max + mean.
//!    QPS = total_iters / wall_clock.
//!
//! With `compare`, runs the same workload against both the
//! pg_transport `tcp_handoff` listener and the cluster's vanilla
//! PG listener, side-by-side. Without a baseline the numbers are
//! uncalibrated.
//!
//! CLI shape (clap derive):
//!
//! ```text
//! bench run     --target HOST:PORT [shared opts]
//! bench compare [--pt-port P] [--pg-port P] [shared opts]
//! ```
//!
//! Shared opts: `--iterations N` (total across workers, default
//! 1000), `--warmup N` (per worker, default 100), `--connections N`
//! (default 1), `--user U`, `--db D`, `--csv FILE`.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use tokio::sync::Barrier;
use tokio_postgres::{Client, NoTls};

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(
    name = "bench",
    about = "pg_transport latency / throughput harness",
    version,
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run bench against a single target (defaults to the
    /// pg_transport listener at 127.0.0.1:5454).
    Run(RunArgs),
    /// Compare pg_transport vs vanilla PG side-by-side. Runs the
    /// same workload against both ports sequentially (no
    /// cross-contention) and prints a delta table.
    Compare(CompareArgs),
}

/// Knobs every workload shape shares. Embedded into both
/// subcommand arg structs via `#[command(flatten)]`.
#[derive(Args, Debug, Clone)]
struct Workload {
    /// Total measured iterations across all workers.
    #[arg(long, default_value_t = 1000)]
    iterations: usize,

    /// Per-worker warmup iterations (not measured).
    #[arg(long, default_value_t = 100)]
    warmup: usize,

    /// Concurrent worker connections. Each worker runs
    /// `iterations / connections` (rounded as needed) measured
    /// round-trips on its own connection. With v0's one-conn-per-
    /// slot model, must be ≤ `pg_transport.backend_pool_size`
    /// (else bench fails after BARRIER_TIMEOUT — see Q10 in
    /// docs/design/roadmap.md).
    #[arg(long, default_value_t = 1)]
    connections: usize,

    /// Postgres user name passed in the StartupMessage.
    #[arg(long, default_value = "postgres")]
    user: String,

    /// Postgres database name passed in the StartupMessage.
    #[arg(long, default_value = "postgres")]
    db: String,

    /// Optional per-sample CSV output: `label,iter,latency_ns`.
    #[arg(long)]
    csv: Option<String>,

    /// Execution backend mode for pg_transport's extended path:
    /// - `spi`    => SPI bridge backend (default)
    /// - `direct` => planner+executor direct backend
    #[arg(long, value_enum, default_value_t = BackendMode::Spi)]
    mode: BackendMode,
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
enum BackendMode {
    Spi,
    Direct,
}

#[derive(Args, Debug)]
struct RunArgs {
    /// Target host:port. Default points at the pg_transport
    /// listener; pass `127.0.0.1:54329` for vanilla PG.
    #[arg(long, default_value = "127.0.0.1:5454")]
    target: String,

    /// Optional label for the run (used in the printed header and
    /// the CSV `label` column). Defaults to `--target`.
    #[arg(long)]
    label: Option<String>,

    #[command(flatten)]
    workload: Workload,
}

#[derive(Args, Debug)]
struct CompareArgs {
    /// pg_transport TCP port.
    #[arg(long, default_value_t = 5454)]
    pt_port: u16,

    /// Vanilla PG TCP port.
    #[arg(long, default_value_t = 54329)]
    pg_port: u16,

    #[command(flatten)]
    workload: Workload,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Run(args) => run_single(args).await,
        Cmd::Compare(args) => run_compare(args).await,
    }
}

// ---------------------------------------------------------------------------
// Single target
// ---------------------------------------------------------------------------

async fn run_single(args: RunArgs) -> Result<()> {
    let w = &args.workload;
    validate_workload(w)?;

    let conn_str = libpq_conn_str(&args.target, &w.user, &w.db);
    eprintln!(
        "bench: target={} iterations={} warmup={} connections={} mode={:?}",
        args.target, w.iterations, w.warmup, w.connections, w.mode,
    );

    let (samples, wall) =
        run_workload(&conn_str, w.iterations, w.warmup, w.connections, w.mode).await?;
    let summary = summarize(&samples, wall);
    let label = args.label.clone().unwrap_or_else(|| args.target.clone());
    print_summary(&label, w.connections, &summary);

    if let Some(path) = &w.csv {
        write_csv(path, &[(label, samples.clone())])?;
        eprintln!("bench: wrote {} samples to {}", samples.len(), path);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Compare
// ---------------------------------------------------------------------------

async fn run_compare(args: CompareArgs) -> Result<()> {
    let w = &args.workload;
    validate_workload(w)?;

    eprintln!(
        "bench compare: pt=127.0.0.1:{} pg=127.0.0.1:{} iterations={} warmup={} connections={} mode={:?}",
        args.pt_port, args.pg_port, w.iterations, w.warmup, w.connections, w.mode,
    );

    let pt_conn = libpq_conn_str(&format!("127.0.0.1:{}", args.pt_port), &w.user, &w.db);
    let pg_conn = libpq_conn_str(&format!("127.0.0.1:{}", args.pg_port), &w.user, &w.db);

    // Run vanilla first as the baseline reference, then pg_transport.
    // Sequential (not parallel) so the two share no contention; the
    // numbers should be a clean delta.
    let (pg_samples, pg_wall) =
        run_workload(&pg_conn, w.iterations, w.warmup, w.connections, w.mode)
            .await
            .context("vanilla PG bench")?;
    let (pt_samples, pt_wall) =
        run_workload(&pt_conn, w.iterations, w.warmup, w.connections, w.mode)
            .await
            .context("pg_transport bench")?;

    let pg_sum = summarize(&pg_samples, pg_wall);
    let pt_sum = summarize(&pt_samples, pt_wall);
    print_compare(w.connections, &pg_sum, &pt_sum);

    if let Some(path) = &w.csv {
        write_csv(
            path,
            &[
                ("vanilla".to_string(), pg_samples),
                ("pg_transport".to_string(), pt_samples),
            ],
        )?;
        eprintln!("bench: wrote {} samples per target to {path}", w.iterations);
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Shared validation + conn-string
// ---------------------------------------------------------------------------

fn validate_workload(w: &Workload) -> Result<()> {
    if w.iterations == 0 {
        bail!("--iterations must be > 0");
    }
    if w.connections == 0 {
        bail!("--connections must be > 0");
    }
    if w.connections > w.iterations {
        bail!(
            "--connections ({}) > --iterations ({}); each worker needs at least one query",
            w.connections,
            w.iterations,
        );
    }
    Ok(())
}

fn libpq_conn_str(target: &str, user: &str, db: &str) -> String {
    // tokio-postgres connect-string format: `host=H port=P user=U dbname=D`.
    let (host, port) = match target.rsplit_once(':') {
        Some((h, p)) => (h, p),
        None => (target, "5454"),
    };
    format!("host={host} port={port} user={user} dbname={db}")
}

// ---------------------------------------------------------------------------
// Workload (unchanged from phase-5+ hand-parser version)
// ---------------------------------------------------------------------------

/// Top-level workload runner: spawn `connections` workers, each
/// with its own tokio-postgres connection. Each worker connects,
/// runs `warmup` SELECT 1 round-trips to prime its slot + plan
/// cache, then BARRIERS with the others. Wall clock starts on
/// barrier release, ends when every worker has finished its share
/// of `iterations` measured round-trips. Returns (all per-query
/// latencies, wall clock of the measured phase).
///
/// QPS is computed by the caller as `iterations / wall_clock` —
/// NOT `iterations / sum(samples)`, which over-counts under
/// concurrency since per-worker samples overlap in real time.
///
/// Barrier timeout: with v0's one-connection-per-slot model
/// ([Q10 in roadmap.md](../../docs/design/roadmap.md) deferred),
/// `connections > backend_pool_size` will deadlock — extra
/// connections queue on a busy slot's UDS and can't even
/// complete their TCP startup, so they never reach the barrier.
/// We time the barrier wait out at 30 s and surface the misconfig
/// as a readable error rather than hanging.
const BARRIER_TIMEOUT: Duration = Duration::from_secs(30);

async fn run_workload(
    conn_str: &str,
    iterations: usize,
    warmup: usize,
    connections: usize,
    mode: BackendMode,
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
            run_one_worker(&conn_str, iters, warmup, barrier, mode).await
        }));
    }

    // Wait for every worker to reach the barrier (i.e. finish its
    // warmup and be ready to measure).
    match tokio::time::timeout(BARRIER_TIMEOUT, barrier.wait()).await {
        Ok(_) => {}
        Err(_) => {
            bail!(
                "barrier timeout after {:?}: only some workers finished warmup. \
                 If `--connections` ({}) > pg_transport.backend_pool_size, the \
                 extra connections queue on a busy slot's UDS and can never \
                 complete startup (v0 limitation; see Q10 in \
                 docs/design/roadmap.md).",
                BARRIER_TIMEOUT,
                connections,
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

async fn run_one_worker(
    conn_str: &str,
    iterations: usize,
    warmup: usize,
    barrier: Arc<Barrier>,
    mode: BackendMode,
) -> Result<Vec<Duration>> {
    let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
        .await
        .with_context(|| format!("connect: {conn_str}"))?;
    let conn_handle = tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("bench: connection task error: {e}");
        }
    });

    set_execution_backend(&client, mode)
        .await
        .context("set pg_transport.execution_backend")?;

    // Warmup uses the same backend mode as measured samples.
    for _ in 0..warmup {
        run_round_trip(&client).await.context("warmup query")?;
    }

    barrier.wait().await;

    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let t = Instant::now();
        run_round_trip(&client).await.context("measured query")?;
        samples.push(t.elapsed());
    }

    drop(client);
    let _ = conn_handle.await;
    Ok(samples)
}

async fn run_round_trip(client: &Client) -> Result<()> {
    // Always use the extended-query path so execution backend mode
    // (SPI vs direct) is actually exercised.
    let _ = client.query_opt("SELECT 1", &[]).await?;
    Ok(())
}

async fn set_execution_backend(client: &Client, mode: BackendMode) -> Result<()> {
    let backend = match mode {
        BackendMode::Spi => "spi",
        BackendMode::Direct => "direct",
    };
    client
        .batch_execute(&format!("SET pg_transport.execution_backend = '{backend}'"))
        .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Summary + presentation
// ---------------------------------------------------------------------------

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
        // nearest-rank percentile, clamped to n-1.
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

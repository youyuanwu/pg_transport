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
//!       [--user USER] [--db DB] [--csv FILE] [--label LABEL]
//! bench compare [--iterations N] [--warmup N]
//!       [--pt-port P] [--pg-port P] [--user USER] [--db DB]
//!       [--csv FILE]
//! ```
//!
//! Defaults: iterations=1000, warmup=100, user=postgres,
//! db=postgres, host=127.0.0.1, pt-port=5454, pg-port=54329.
//!
//! Phase ≥ 6 will grow this: concurrency knob, multiple statement
//! shapes, baseline matrix (TCP/UDS, cold/warm slot).

use std::env;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use tokio_postgres::NoTls;

#[tokio::main(flavor = "current_thread")]
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
        "bench: target={} iterations={} warmup={}",
        cfg.target, cfg.iterations, cfg.warmup
    );

    let samples = run_workload(&conn_str, cfg.iterations, cfg.warmup).await?;
    let summary = summarize(&samples);
    print_summary(
        &cfg.label.clone().unwrap_or_else(|| cfg.target.clone()),
        &summary,
    );

    if let Some(path) = &cfg.csv {
        write_csv(
            path,
            &[(
                cfg.label.clone().unwrap_or_else(|| cfg.target.clone()),
                samples.clone(),
            )],
        )?;
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
        "bench compare: pt={} pg={} iterations={} warmup={}",
        cfg.pt_target(),
        cfg.pg_target(),
        cfg.iterations,
        cfg.warmup,
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
    let pg_samples = run_workload(&pg_conn, cfg.iterations, cfg.warmup)
        .await
        .context("vanilla PG bench")?;
    let pt_samples = run_workload(&pt_conn, cfg.iterations, cfg.warmup)
        .await
        .context("pg_transport bench")?;

    let pg_sum = summarize(&pg_samples);
    let pt_sum = summarize(&pt_samples);

    print_compare(&pg_sum, &pt_sum);

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

/// Run `iterations` simple-query `SELECT 1` round-trips on a fresh
/// connection, after `warmup` warm-up round-trips. Returns the raw
/// latency samples (one per measured iteration).
async fn run_workload(conn_str: &str, iterations: usize, warmup: usize) -> Result<Vec<Duration>> {
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
    total: Duration,
    min: Duration,
    p50: Duration,
    p95: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
    qps: f64,
}

fn summarize(samples: &[Duration]) -> Summary {
    assert!(
        !samples.is_empty(),
        "summarize requires at least one sample"
    );
    let mut sorted: Vec<Duration> = samples.to_vec();
    sorted.sort();
    let n = sorted.len();
    let total: Duration = samples.iter().copied().sum();
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
        total,
        min: sorted[0],
        p50: pct(50),
        p95: pct(95),
        p99: pct(99),
        max: sorted[n - 1],
        mean,
        qps: n as f64 / total.as_secs_f64(),
    }
}

fn fmt_us(d: Duration) -> String {
    // Microseconds with 1 decimal — convenient for the latency
    // numbers we're likely to see (single-digit to low-hundreds µs).
    format!("{:.1}", d.as_secs_f64() * 1_000_000.0)
}

fn print_summary(label: &str, s: &Summary) {
    eprintln!();
    eprintln!("--- {label} ---");
    eprintln!("n        : {}", s.n);
    eprintln!("total    : {:?}", s.total);
    eprintln!("qps      : {:>8.1}", s.qps);
    eprintln!("min      : {:>8} µs", fmt_us(s.min));
    eprintln!("p50      : {:>8} µs", fmt_us(s.p50));
    eprintln!("p95      : {:>8} µs", fmt_us(s.p95));
    eprintln!("p99      : {:>8} µs", fmt_us(s.p99));
    eprintln!("max      : {:>8} µs", fmt_us(s.max));
    eprintln!("mean     : {:>8} µs", fmt_us(s.mean));
}

fn print_compare(pg: &Summary, pt: &Summary) {
    let ratio = |a: Duration, b: Duration| {
        if b.as_nanos() == 0 {
            "n/a".to_string()
        } else {
            format!("{:.2}x", a.as_secs_f64() / b.as_secs_f64())
        }
    };
    eprintln!();
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
                [--user U] [--db D] [--csv FILE] [--label L]

    {prog} compare [--pt-port P] [--pg-port P] [--iterations N]
                [--warmup N] [--user U] [--db D] [--csv FILE]

Defaults: iterations=1000, warmup=100, user=postgres, db=postgres,
          host=127.0.0.1, pt-port=5454, pg-port=54329.

Examples:
    bench --target 127.0.0.1:5454 --iterations 5000
    bench compare --iterations 5000 --csv /tmp/bench.csv
"
    );
}

//! End-to-end test harness for `pg_transport`.
//!
//! Spins up a temporary Postgres cluster with `pg_transport` loaded
//! via `shared_preload_libraries`, then hands out
//! [`tokio_postgres::Client`]s that connect through the pg_transport
//! wire listener ([`PT_PORT`]) for tests to drive scenarios against.
//!
//! ## Usage
//!
//! ```no_run
//! # async fn run() -> anyhow::Result<()> {
//! let cluster = e2e::Cluster::shared().await;
//! let client = cluster.connect("postgres").await?;
//! // Phase-4b: SIMPLE query only — see "Wire protocol scope" below.
//! let v = e2e::first_text_cell(&client.simple_query("SELECT 1").await?);
//! assert_eq!(v.as_deref(), Some("1"));
//! # Ok(()) }
//! ```
//!
//! The cluster is started **once per process** via a `OnceCell` — all
//! `#[tokio::test]` functions in the same binary share it. Cargo runs
//! each `tests/*.rs` file as a separate binary, so multiple test files
//! pay the boot cost N times; keep e2e tests consolidated in one
//! `tests/` file when feasible.
//!
//! ## Wire protocol scope (v0 / phase 4b)
//!
//! pg_transport currently implements **only the simple-query path**
//! (`'Q'` message). Extended query (`Parse` / `Bind` / `Describe` /
//! `Execute`) lands in roadmap phase 9. That means tokio-postgres'
//! `query` / `query_one` / `execute` methods will all fail against the
//! pg_transport port with `FATAL: This feature is not implemented` —
//! they go through the extended-query path even for parameter-less
//! statements.
//!
//! **Use [`tokio_postgres::Client::simple_query`]** for any SQL you
//! send through [`Cluster::connect`]. Use [`first_text_cell`] /
//! [`text_column`] to pull values out of the resulting
//! `Vec<SimpleQueryMessage>`. [`Cluster::admin_connect`] talks to
//! vanilla PG and supports the full protocol — use it for setup
//! shaped queries that need parameter binding.
//!
//! ## Assumptions
//!
//! * `cargo pgrx install` has been run against the same Postgres install
//!   so `pg_transport.so` + `pg_transport.control` are in PGRX's share /
//!   lib trees. The `just e2e` recipe handles this.
//! * `$PGRX_HOME/config.toml` lists a `pg18` line (i.e. `just init` has
//!   been run). Override with `PG_TRANSPORT_E2E_PG_BIN=/path/to/bin`.
//! * Ports [`PT_PORT`] (5454, hard-coded by `frontend.rs::PHASE_3_TCP_BIND`)
//!   and [`PG_PORT`] (54330) are free on `127.0.0.1`. Cannot run
//!   alongside `just bench` / `just pgbench` — they all bind 5454.
//!
//! ## Lifecycle
//!
//! * [`Cluster::new`] is idempotent: it stops any prior cluster at the
//!   same data dir, wipes state, then `initdb`s + starts fresh.
//! * On process exit, Rust does **not** run `Drop` for `static` values,
//!   so the cluster is leaked at the process level. The `just e2e`
//!   recipe wraps `cargo test` with a `pkill -f pg_transport_e2e` +
//!   `rm -rf` belt-and-suspenders. Next test run will pkill + wipe in
//!   [`Cluster::new`] anyway, so leaks are self-healing across runs.
//!
//! See [`docs/design/testing.md`](../../../../docs/design/testing.md) §3.3
//! for where this fits in the overall test pyramid.

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::OnceCell;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

/// Bind address the FE bgworker uses for the pg_transport wire
/// listener in v0. Mirrors `crates/core/src/frontend.rs::PHASE_3_TCP_BIND`.
/// Phase ≥ 7 will move this into the `pg_transport.transports` GUC and
/// this constant should be deleted in favour of a per-cluster value.
pub const PT_HOST: &str = "127.0.0.1";

/// pg_transport wire listener port. **Hard-coded** in v0 — cannot run
/// two e2e clusters simultaneously.
pub const PT_PORT: u16 = 5454;

/// Vanilla PG port for the temp cluster. Kept distinct from `just bench`'s
/// (54328) so an accidentally-leaked bench cluster doesn't collide
/// with e2e on the *admin* port (the wire port collision is unavoidable).
pub const PG_PORT: u16 = 54330;

/// `pg_transport.backend_pool_size`. Picked larger than the v0 default
/// (2) to give parallel `#[tokio::test]`s connection headroom before
/// they queue on slot availability.
const POOL_SIZE: u32 = 8;

/// Wall time we'll wait for the pg_transport listener to come up after
/// `pg_ctl start`. The wire bgworker starts asynchronously after the
/// postmaster, so `pg_ctl -w` doesn't cover it.
const READY_TIMEOUT: Duration = Duration::from_secs(15);

/// Polling interval while waiting for the pg_transport listener.
const READY_POLL: Duration = Duration::from_millis(100);

/// Handle to a running temp Postgres cluster with `pg_transport`
/// loaded. Obtain via [`Cluster::shared`].
pub struct Cluster {
    pgdata: PathBuf,
    pg_bin: PathBuf,
    /// Path of the server log; surfaced via [`Cluster::log_path`] so
    /// test failures can dump it for diagnosis.
    log: PathBuf,
}

static SHARED: OnceCell<Arc<Cluster>> = OnceCell::const_new();

impl Cluster {
    /// Process-wide shared cluster. First call boots it (blocking the
    /// caller for ~1–2 s on a warm machine); subsequent calls return
    /// the same handle. Panics on bootstrap failure — there's nothing
    /// useful a test can do if the cluster won't start.
    pub async fn shared() -> Arc<Cluster> {
        SHARED
            .get_or_init(|| async {
                let cluster = Cluster::new()
                    .await
                    .expect("e2e cluster bootstrap failed — see stderr for details");
                Arc::new(cluster)
            })
            .await
            .clone()
    }

    /// Bring up a fresh cluster. Idempotent — stops any prior
    /// instance at the same `pgdata` first.
    ///
    /// Prefer [`Cluster::shared`] in tests; this is exposed for the
    /// rare case where a test wants an isolated cluster (and is willing
    /// to pay the startup cost).
    pub async fn new() -> Result<Self> {
        let pg_bin = resolve_pg_bin()
            .context("locate Postgres bin/ — run `just init` or set PG_TRANSPORT_E2E_PG_BIN")?;

        let root = std::env::temp_dir().join("pg_transport_e2e");
        let pgdata = root.join("pgdata");
        let log = root.join("server.log");
        let sockets = std::env::temp_dir().join("pg_transport_e2e_sockets");

        // Idempotent reset. We intentionally swallow errors here — a
        // missing pgdata, an already-stopped cluster, and "no
        // processes matched" are all fine.
        let _ = Command::new(pg_bin.join("pg_ctl"))
            .args([
                "-D",
                pgdata.to_string_lossy().as_ref(),
                "-m",
                "immediate",
                "stop",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let _ = Command::new("pkill")
            .args(["-9", "-f", "pg_transport_e2e"])
            .status();
        // Give the postmaster a beat to release the port after
        // immediate-stop; otherwise pg_ctl start can race.
        tokio::time::sleep(Duration::from_millis(200)).await;

        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&sockets);
        std::fs::create_dir_all(&root).context("mkdir cluster root")?;
        std::fs::create_dir_all(&sockets).context("mkdir socket dir")?;

        // initdb. `--no-instructions` suppresses the "you can now
        // start the database server using:" footer.
        let out = Command::new(pg_bin.join("initdb"))
            .args([
                "-D",
                pgdata.to_string_lossy().as_ref(),
                "-U",
                "postgres",
                "--auth-local=trust",
                "--auth-host=trust",
                "--no-instructions",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .context("spawn initdb")?;
        if !out.status.success() {
            bail!(
                "initdb failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
        }

        // Append our overrides to the freshly-written postgresql.conf.
        // initdb wrote it; we want our settings to take precedence
        // (later settings win in PG), so append rather than rewrite.
        let conf = format!(
            "\n\
             # ---- pg_transport e2e harness ----\n\
             shared_preload_libraries = 'pg_transport'\n\
             port = {PG_PORT}\n\
             unix_socket_directories = '{sockets}'\n\
             logging_collector = off\n\
             log_min_messages = log\n\
             # Comfortably above 1 FE + POOL_SIZE slots + PG internal\n\
             # bgworkers (logical-rep launcher, autovac, etc.).\n\
             max_worker_processes = 32\n\
             pg_transport.backend_pool_size = {POOL_SIZE}\n\
             # auth_source is required by _PG_init() since phase 7.\n\
             pg_transport.auth_source = 'pg_hba'\n",
            sockets = sockets.display(),
        );
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(pgdata.join("postgresql.conf"))
            .context("open postgresql.conf for append")?;
        f.write_all(conf.as_bytes())
            .context("append cluster overrides")?;
        drop(f);

        // Start. `-w` waits for the postmaster (not our bgworkers).
        let out = Command::new(pg_bin.join("pg_ctl"))
            .args([
                "-D",
                pgdata.to_string_lossy().as_ref(),
                "-l",
                log.to_string_lossy().as_ref(),
                "-w",
                "start",
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .context("spawn pg_ctl start")?;
        if !out.status.success() {
            // Surface server log on start failure — most useful
            // diagnostic for "pg_transport must be in SPL" type
            // mistakes.
            let log_tail = std::fs::read_to_string(&log).unwrap_or_default();
            bail!(
                "pg_ctl start failed: {}\n--- server log ---\n{}",
                String::from_utf8_lossy(&out.stderr).trim(),
                log_tail
            );
        }

        let cluster = Self {
            pgdata,
            pg_bin,
            log,
        };

        // Poll the pg_transport listener until it accepts a connection
        // or we time out. The wire bgworker starts asynchronously
        // *after* the postmaster is ready, so a naive `connect()` here
        // races and intermittently fails with ECONNREFUSED.
        cluster
            .wait_ready()
            .await
            .context("wait for pg_transport listener")?;

        Ok(cluster)
    }

    /// Connect a tokio-postgres client to the **pg_transport** wire
    /// listener (port [`PT_PORT`]). The connection task is spawned
    /// onto the current tokio runtime; the returned `Client` is
    /// detached from it and remains valid until dropped.
    pub async fn connect(&self, dbname: &str) -> Result<Client> {
        connect_at(PT_PORT, dbname).await
    }

    /// Connect to the **vanilla PG** listener (port [`PG_PORT`]).
    /// Useful for admin work that goes around pg_transport — e.g.
    /// `CREATE DATABASE`, `CREATE EXTENSION`, or assertions about
    /// real PG behaviour to compare against.
    pub async fn admin_connect(&self, dbname: &str) -> Result<Client> {
        connect_at(PG_PORT, dbname).await
    }

    /// Path of the cluster's server log. Tests may want to assert on
    /// it, or dump it on failure.
    pub fn log_path(&self) -> &std::path::Path {
        &self.log
    }

    /// Run a `psql -c` against the pg_transport port (for assertions
    /// that want raw psql output rather than a tokio-postgres Row).
    /// Returns combined stdout+stderr. Non-zero exit is not an error
    /// here — caller decides what to do.
    pub fn psql(&self, dbname: &str, sql: &str) -> Result<String> {
        let out = Command::new(self.pg_bin.join("psql"))
            .args([
                "-h",
                PT_HOST,
                "-p",
                &PT_PORT.to_string(),
                "-U",
                "postgres",
                "-d",
                dbname,
                "-c",
                sql,
            ])
            .env("PGSSLMODE", "disable")
            .output()
            .context("spawn psql")?;
        let mut combined = String::from_utf8_lossy(&out.stdout).into_owned();
        combined.push_str(&String::from_utf8_lossy(&out.stderr));
        Ok(combined)
    }

    /// Poll the pg_transport listener with `tokio_postgres::connect`
    /// until it succeeds or we exhaust [`READY_TIMEOUT`].
    async fn wait_ready(&self) -> Result<()> {
        let deadline = tokio::time::Instant::now() + READY_TIMEOUT;
        // Initial `None` is overwritten before being read on every
        // path that reaches the timeout branch (we only get there
        // after at least one Err arm fired). Allow the assignment
        // warning rather than restructure into a less clear loop.
        #[allow(unused_assignments)]
        let mut last_err: Option<tokio_postgres::Error> = None;
        loop {
            match connect_at(PT_PORT, "postgres").await {
                Ok(client) => {
                    // Sanity-ping the wire to make sure SPI bridge is
                    // also live, not just the TCP accept loop.
                    if let Err(e) = client.simple_query("SELECT 1").await {
                        let log = std::fs::read_to_string(&self.log).unwrap_or_default();
                        bail!(
                            "pg_transport listener accepted but SELECT 1 failed: {e}\n\
                             --- server log ---\n{log}"
                        );
                    }
                    return Ok(());
                }
                Err(e) => {
                    // tokio_postgres::Error doesn't implement Clone;
                    // we can keep the most recent for the diagnostic
                    // by downcasting to anyhow first.
                    last_err = e.downcast::<tokio_postgres::Error>().ok();
                }
            }
            if tokio::time::Instant::now() >= deadline {
                let log = std::fs::read_to_string(&self.log).unwrap_or_default();
                return Err(anyhow!(
                    "pg_transport listener at {PT_HOST}:{PT_PORT} did not become ready \
                     within {READY_TIMEOUT:?} (last error: {last_err:?})\n\
                     --- server log ---\n{log}"
                ));
            }
            tokio::time::sleep(READY_POLL).await;
        }
    }
}

/// Stops the cluster on `Drop`. Note: Rust does **not** run `Drop`
/// for `static` values, so [`SHARED`] never invokes this in practice —
/// the `just e2e` recipe's post-`cargo test` cleanup is the actual
/// safety net. Kept here for the [`Cluster::new`]-only (non-shared)
/// usage path.
impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new(self.pg_bin.join("pg_ctl"))
            .args([
                "-D",
                self.pgdata.to_string_lossy().as_ref(),
                "-m",
                "immediate",
                "stop",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

async fn connect_at(port: u16, dbname: &str) -> Result<Client> {
    let conn_str =
        format!("host={PT_HOST} port={port} user=postgres dbname={dbname} sslmode=disable");
    let (client, conn) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .with_context(|| format!("tokio_postgres::connect to {PT_HOST}:{port}"))?;
    // Drive the connection in the background. If the task ends with
    // an error, log it to stderr — tests usually drop the Client at
    // end-of-scope and don't await this.
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("e2e: tokio-postgres connection task ended: {e}");
        }
    });
    Ok(client)
}

/// Pull the first cell of the first row out of a
/// `simple_query` result. Returns `None` if there were no rows.
///
/// `SimpleQueryMessage::Row` always exposes columns as `Option<&str>`
/// in tokio-postgres — the wire protocol delivers everything in text
/// format for simple queries, which is exactly what pg_transport's
/// phase-4b SPI bridge emits via `SPI_getvalue`.
pub fn first_text_cell(msgs: &[SimpleQueryMessage]) -> Option<String> {
    for m in msgs {
        if let SimpleQueryMessage::Row(r) = m {
            return r.get(0).map(str::to_owned);
        }
    }
    None
}

/// Collect a single text column across all rows of a `simple_query`
/// result. NULL cells are dropped (use a more specific helper if you
/// need to distinguish).
pub fn text_column(msgs: &[SimpleQueryMessage]) -> Vec<String> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => r.get(0).map(str::to_owned),
            _ => None,
        })
        .collect()
}

/// Resolve the directory containing `pg_config` / `initdb` / `pg_ctl`.
///
/// Order of preference:
/// 1. `PG_TRANSPORT_E2E_PG_BIN` env var (manual override).
/// 2. Parse `$PGRX_HOME/config.toml` for the `pg18` entry and take
///    its dirname. Mirrors the `_pg-config` awk helper in the root
///    `Justfile`.
fn resolve_pg_bin() -> Result<PathBuf> {
    if let Ok(p) = std::env::var("PG_TRANSPORT_E2E_PG_BIN") {
        return Ok(PathBuf::from(p));
    }

    let pgrx_home = std::env::var("PGRX_HOME")
        .or_else(|_| std::env::var("HOME").map(|h| format!("{h}/.pgrx")))
        .map_err(|_| anyhow!("neither PGRX_HOME nor HOME is set"))?;
    let cfg_path = PathBuf::from(&pgrx_home).join("config.toml");
    let cfg = std::fs::read_to_string(&cfg_path)
        .with_context(|| format!("read {}", cfg_path.display()))?;

    // Minimal parse: look for `pg18 = "/path/to/pg_config"`. We don't
    // pull in a TOML crate just for this; the file is well-formed and
    // pgrx writes it.
    for line in cfg.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("pg18") {
            let rest = rest.trim_start();
            if let Some(rest) = rest.strip_prefix('=') {
                let val = rest.trim().trim_matches('"');
                let pg_config = PathBuf::from(val);
                let bin = pg_config
                    .parent()
                    .ok_or_else(|| anyhow!("pg_config path has no parent: {val}"))?;
                return Ok(bin.to_path_buf());
            }
        }
    }
    bail!(
        "no `pg18` entry in {} — run `just init` first",
        cfg_path.display()
    )
}

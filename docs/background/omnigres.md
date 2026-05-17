# Omnigres — Architecture Review

> Source: [github.com/omnigres/omnigres](https://github.com/omnigres/omnigres) (Apache-2.0)
> Documentation: [docs.omnigres.org](https://docs.omnigres.org/)
> Primary author: Yurii Rashkovskii ([@yrashk](https://github.com/yrashk))
> Languages: C++ (~46%), C (~44%), PL/pgSQL (~7%)

This document is an **architecture-focused** review of Omnigres, with an
emphasis on the components that are directly relevant to building an
alternative in-process listener for Postgres: `omni`, `omni_httpd`,
`omni_worker`, and the supporting infrastructure layers. It is a reading
companion to the source — not a usage guide. For installation and tutorial
material see the official docs.

---

## 1. What Omnigres is, in one paragraph

Omnigres is a *collection* of Postgres extensions that together turn a single
PG instance into a full application runtime: HTTP/WebSocket termination,
job/worker pools, REST/GraphQL surfaces, auth/session/ledger building blocks,
a virtual filesystem, an HTTP client, and a small framework layer (`omni`)
that handles shared-memory management and hot-loading. The two pieces that
matter for this workspace's research goal are the **HTTP listener bgworker**
(`omni_httpd`) and the **generic worker pool** (`omni_worker`).

The umbrella repository is a CMake monorepo of ~50+ extensions under
`extensions/`. Each extension is buildable independently. There is no
single executable — every component installs as a normal Postgres extension
via `CREATE EXTENSION`.

---

## 2. Where the relevant code lives

```
omnigres/
├── omni/                       # core framework lib: shmem, hooks, modules
├── apex/                       # shared-memory management primitive
├── deps/                       # bundled third-party (h2o, etc.)
├── libpgaug/                   # PG augmentations / compat helpers
├── libgluepg_stc/              # generic data structures
├── extensions/
│   ├── omni/                   # the framework extension
│   ├── omni_httpd/             # HTTP listener bgworker (focus)
│   ├── omni_worker/            # generic worker pool (focus)
│   ├── omni_httpc/             # outbound HTTP client (uses h2o)
│   ├── omni_service/           # service management surface
│   ├── omni_shmem/             # user-visible shared memory
│   ├── omni_var/               # transaction-scoped variables
│   ├── omni_vfs/               # virtual filesystem
│   ├── omni_sqlite/            # embedded SQLite
│   ├── omni_python/            # Python language host
│   ├── omni_auth, omni_session, omni_id, omni_seq, omni_ledger, ...
│   └── (≈50 more)
├── pg_yregress/                # YAML-based regression test framework
└── docker/                     # container images
```

The pieces a transport/listener researcher needs to study are bolded above:
`omni`, `omni_httpd`, `omni_worker`. The rest are application-stack
extensions built **on top of** those primitives.

---

## 3. Core framework: `omni`

`omni` is the layer that lets the other extensions share shared memory
safely, register hooks, and survive cluster-internal upgrades.

Key responsibilities:

- **Shared memory management.** `omni` allocates and tracks shared memory
  regions on behalf of other extensions. Solves the "every extension wants to
  hook `shmem_request_hook` and `shmem_startup_hook`" coordination problem
  that PG core leaves to extension authors.
- **Module lifecycle / hot upgrade.** Originally the dedicated `omni_ext`
  extension handled this; the design has since folded into `omni`. The point
  is that an extension's `.so` can be replaced and re-initialised without a
  postmaster restart — important for an iteration-heavy development loop.
- **Bgworker lifecycle helpers.** Patterns for "always-on" workers that the
  framework will restart on crash and re-spawn after upgrade.

You only need to understand `omni` if you want either (a) the same hot-reload
behaviour or (b) the cross-extension shared-memory plumbing. For a fresh
research framework, neither is necessary on day one — but the *patterns* are
the canonical reference for "how do I do this safely in PG extension land".

---

## 4. The listener bgworker: `omni_httpd`

This is the part that most directly resembles the design we want for an
alternative-transport framework. The architecture is officially documented at
[docs.omnigres.org/omni_httpd/architecture](https://docs.omnigres.org/omni_httpd/architecture/);
the summary below is paraphrased and annotated.

### 4.1 Process model

```
                   Postgres postmaster
                         │
       ┌─────────────────┼─────────────────────────────────┐
       │                 │                                 │
       ▼                 ▼                                 ▼
  Regular FE/BE     Master bgworker            HTTP worker bgworker × N
   backends         (omni_httpd.master)        ┌────────────────────────┐
   (port 5432)            │                    │ Main thread (PG ctx)   │
                          │ spawns + reloads   │  - route handler exec  │
                          ▼                    │  - SPI / SQL           │
                                               │                        │
                                               │ I/O thread (no PG)     │
                                               │  - h2o event loop      │
                                               │  - TCP accept/recv/send│
                                               │  - HTTP/1.1, /2 parse  │
                                               └────────────────────────┘
```

Three concrete properties from the docs and code:

1. **A master worker** owns configuration. It starts and supervises HTTP
   worker bgworkers. Reloads are triggered by changes to the
   `omni_httpd.listeners` table or by calling
   `omni_httpd.reload_configuration()`.
2. **N HTTP worker bgworkers** are spawned (`omni_httpd.http_workers` GUC;
   defaults to online CPU count, capped at `max_worker_processes`). Each is
   a **full Postgres backend** that can run handler queries via SPI.
3. **Per-worker thread split.** Inside each HTTP worker there are two
   threads:
   - **Main thread** — the PG backend context. Runs route handlers (which
     are PL/pgSQL or other-language functions registered via
     `omni_httpd.urlpattern_router`).
   - **I/O thread** — runs the h2o HTTP event loop. **Strictly prohibited
     from calling into Postgres.** This is the same data-plane / control-plane
     split that any DPDK/AF_XDP integration needs, and it's the thing to
     internalise from Omnigres before designing your own framework.

### 4.2 HTTP termination

The HTTP server is [h2o](https://github.com/h2o/h2o) — a high-performance
C HTTP/1.1, HTTP/2, and HTTP/3 server. h2o is bundled under `deps/` rather
than depended on externally. h2o handles:

- TCP / TLS accept loop on the I/O thread.
- HTTP/1.1 request parsing.
- HTTP/2 stream multiplexing.
- HTTP/3 over QUIC (this is what `omni_httpc` uses for its outbound side;
  inbound HTTP/3 is in progress per recent commits).

### 4.3 Cross-worker dispatch (HTTP/2 multiplexing)

Because HTTP/2 lets a single connection carry many concurrent streams, a
single worker handling one connection can become a bottleneck. Omnigres
mitigates by **re-dispatching busy workers' requests to other workers**:
when an HTTP/2+ request arrives at a worker whose main thread is currently
running a handler, the I/O thread can forward the request to a different
worker that is idle. On HTTP/1, since pipelining doesn't help, it just waits.

This is a small piece of design with outsized lessons:

- The dispatcher and the executor are *not* hard-bound. A request can be
  produced on worker A and handled on worker B.
- Routing decisions live below the protocol layer (in h2o), not in PG.
- Coordination is done in shared memory — there is no "thundering herd
  accept" because the master pre-distributes listening fds, but there *is*
  inter-worker forwarding.

### 4.4 Configuration surface

Everything that would normally be `postgresql.conf` / a config file lives in
SQL tables:

- `omni_httpd.listeners` — listening address, port, TLS, protocol set.
- `omni_httpd.urlpattern_router` — `(URLPattern, handler regproc)` rows.
- `omni_httpd.http_workers` (GUC) — pool size.
- `omni_httpd.temp_dir` (GUC) — path for Unix-domain sockets used for
  inter-worker fd-passing.
- `omni_httpd.start()` / `omni_httpd.stop()` procedures — full server
  lifecycle from SQL.

Treating configuration as SQL state (so it benefits from transactions,
MVCC, and replication) is one of Omnigres' core stylistic decisions. The
master worker watches catalog invalidation on these tables (the same trick
needed for any user-table-driven extension cache; see also the user-memory
notes on `CacheInvalidateRelcache`).

### 4.5 What `omni_httpd` does *not* try to do

- It does **not** replace Postgres's FE/BE listener on 5432. The
  postmaster's TCP listener keeps running; the HTTP listener is *additional*.
- It does **not** make the bgworkers visible as ordinary PG backends to
  clients — they're internal. Real client connections to 5432 go through the
  normal postmaster `fork()` path.
- It does **not** support `pq_redirect_to_shm_mq`-style FE/BE protocol
  return — because there's no FE/BE client; the worker just runs the
  handler synchronously via SPI and serialises the result as HTTP.

This is the crucial *similarity* and *difference* vs. `pg_background`:

| Concern              | `pg_background`                       | `omni_httpd`                                    |
| -------------------- | ------------------------------------- | ----------------------------------------------- |
| Spawns workers       | Dynamic, per request                  | Pre-spawned pool, N=CPU count                   |
| External entry point | None (SQL caller)                     | TCP listener (h2o on its own port)              |
| Worker is a backend  | Yes (`BackgroundWorkerInitialize…`)   | Yes (same)                                      |
| Result transport     | DSM + shm_mq + `pq_redirect_to_shm_mq`| Direct SPI in-worker; serialised to HTTP        |
| Threading            | Single-threaded (PG-only)             | Two-thread (PG main + h2o I/O)                  |
| Lifecycle            | Per request                           | Long-lived workers; on-demand request handling  |

Reading both projects in sequence is the right order: `pg_background` for
the per-request DSM/shm_mq mechanics, `omni_httpd` for the long-lived
listener-bgworker structure and the data-plane thread split.

---

## 5. The generic worker pool: `omni_worker`

`omni_worker` factors out what `omni_httpd` does for HTTP into a reusable
abstraction. From the docs:

> `omni_worker` provides a generalized Postgres worker pool that can execute
> arbitrary workloads within individual backend contexts. Its architecture
> allows other extensions to add native-compiled handlers for arbitrary
> messages.

In practice this means:

- A pool of bgworker backends, each capable of executing handler logic.
- A "message" abstraction — handlers are registered in C and receive
  messages from a shared-memory queue.
- A built-in `omni_worker.sql_handler` that takes SQL strings and runs them
  through SPI — i.e., a `pg_background`-equivalent built on the generic pool.

The architectural value of `omni_worker` is the decoupling:

- **Producer side** can be anything (a SQL function, an HTTP request from
  `omni_httpd`, a webhook, a timer).
- **Handler side** is a C function registered by another extension.
- **The pool itself** owns the worker bgworkers, their lifecycle, and the
  message routing.

For a transport-research framework, `omni_worker` is the closest existing
template for the **executor pool layer** we want. The differences come down
to:

- We want the executor pool to be specifically optimised for relaying FE/BE
  protocol frames (`pq_redirect_to_shm_mq`), which `omni_worker.sql_handler`
  does not do — it just runs SPI and returns results in handler-defined
  formats.
- We want the executor pool to be transport-agnostic; `omni_worker` is
  already protocol-agnostic, so this matches.

---

## 6. Supporting primitives worth knowing

These are not strictly listener-related but shape how an Omnigres-style
framework feels to use, and several have direct analogues we'll want:

| Extension       | What it gives you                                       | Why it matters here                            |
| --------------- | ------------------------------------------------------- | ---------------------------------------------- |
| `omni_shmem`    | SQL surface for user-visible shared memory regions      | We need it for cross-worker state              |
| `omni_var`      | Transaction-scoped variables                            | Per-connection state in the dispatcher         |
| `omni_service`  | Generic service supervisor                              | Lifecycle for the listener bgworker            |
| `omni_id`       | Strongly-typed identity columns                         | Connection IDs in a tidy form                  |
| `omni_seq`      | Distributed-friendly sequences (UUIDv7-ish)             | Request IDs                                    |
| `omni_vfs`      | Pluggable virtual filesystems                           | Useful pattern: the `_vfs_v0` plugin ABI       |
| `omni_httpc`    | Outbound HTTP client (h2o-based)                        | Mirror of `omni_httpd` for outbound traffic    |
| `pg_yregress`   | YAML-based regression test framework                    | Pattern for testing async listener behaviour   |

`omni_vfs` deserves particular attention: it defines a plugin ABI (`_vfs_v0`)
that other extensions implement to provide concrete VFSes (local FS, S3, FTP,
etc.). The mechanism — a versioned ABI symbol, dynamic discovery, and an
extension-as-plugin packaging model — is exactly the pattern we want to
mirror for transport and protocol plugins.

---

## 7. Build, packaging, language choices

- **Build system**: CMake (≥ 3.25.1). Each extension is a CMake target; the
  top-level controls which subset gets built via `-DOMNIGRES_INCLUDE` or
  `-DOMNIGRES_EXCLUDE`.
- **Languages**: Mostly C and C++. Some PL/pgSQL. They've written their own
  C++ ↔ PG bridge layer called `cppgres` (see `templates/` and various
  extension subdirs).
- **Bundled deps**: `deps/` carries h2o (and historically others), pinned to
  specific revisions, often patched.
- **PG version support**: 14 – 18.
- **Distribution**: Container images at `ghcr.io/omnigres/omnigres-<pgver>`.
  Also a `pkgs/` script for building distro packages.

The fact that Omnigres uses CMake + C++ rather than PGXS + C is a deliberate
choice driven by the size of the codebase. For a Rust/pgrx framework, the
analogous choice is Cargo workspace + pgrx, with the same "monorepo of small
crates" feel.

---

## 8. Testing: `pg_yregress`

Worth a quick mention because it's the project's own framework for testing
extensions, and a transport/protocol researcher will need something
equivalent:

- Tests are YAML files describing a database state and expected results.
- The runner can spin up a fresh cluster per test, install extensions, drive
  inputs, and assert outputs.
- Used across the monorepo for every extension's regression suite.

Standard PG `regress` (`make installcheck`) cannot adequately drive
listener-bgworker behaviour — you need a harness that owns the cluster
lifecycle and can probe arbitrary ports. `pg_yregress` solves the same
problem.

---

## 9. What Omnigres validates for our design

Re-reading the previous chat thread's framework sketch through the lens of
what Omnigres actually does in production:

| Hypothesis from our design discussion                       | Omnigres confirms?                                          |
| ----------------------------------------------------------- | ----------------------------------------------------------- |
| Run the listener in a bgworker, not the postmaster.         | ✓ (`omni_httpd` master + HTTP worker pool)                  |
| Pre-spawn an executor pool sized to CPU count.              | ✓ (`omni_httpd.http_workers` default = cores)               |
| Data-plane I/O on a separate thread; PG only on main.       | ✓ (h2o thread "strictly prohibited from calling Postgres")  |
| Cross-worker forwarding for protocol multiplexing.          | ✓ (HTTP/2 re-dispatch)                                      |
| Configuration as catalog state, not config files.           | ✓ (`omni_httpd.listeners`, router tables)                   |
| SQL-level lifecycle control (`start()`/`stop()`).           | ✓                                                           |
| Plugin ABI as the extensibility mechanism.                  | ✓ (`omni_vfs._vfs_v0` pattern)                              |
| Generic worker pool abstraction shared across listeners.    | ✓ (`omni_worker`)                                           |

What Omnigres *does not* try and we will need to do ourselves:

- **Replace/extend the FE/BE wire protocol** on a non-5432 socket. Omnigres
  serves HTTP; we want to terminate FE/BE (and other custom protocols) so
  `psql` and libpq clients can connect to *our* listener.
- **`pq_redirect_to_shm_mq` style frame relay** from executor bgworker back
  to a non-libpq client. Omnigres handlers produce HTTP responses
  in-handler; we want the executor to produce FE/BE frames that the
  dispatcher relays to whatever transport the client is on.
- **Pluggable transports below the HTTP/PG layer** (io_uring, QUIC, DPDK,
  AF_XDP, RDMA). Omnigres uses h2o's built-in transports only.

---

## 10. Suggested reading order

If you want to *use* the Omnigres code as a reference while building:

1. `extensions/omni/` — framework basics: shmem coordination, hooks.
2. `extensions/omni_worker/` — the generic pool. This is the closest
   architectural neighbour to our executor pool.
3. `extensions/omni_httpd/`:
   - The master worker source — bgworker registration, configuration
     watching, child-worker supervision.
   - The HTTP worker source — the two-thread split, h2o event loop
     integration, SPI handler invocation.
4. `extensions/omni_vfs/` — for the plugin ABI pattern (`_vfs_v0`).
5. `pg_yregress/` — for testing patterns once you have a listener of your
   own to drive.

---

## References

- Repo: <https://github.com/omnigres/omnigres>
- Docs: <https://docs.omnigres.org/>
- `omni_httpd` architecture page: <https://docs.omnigres.org/omni_httpd/architecture/>
- `omni_worker` intro: <https://docs.omnigres.org/omni_worker/intro/>
- High-level architecture diagram: <https://docs.omnigres.org/hl_architecture.png>
- h2o (HTTP server used by `omni_httpd`): <https://github.com/h2o/h2o>
- Project blog: <https://blog.omnigres.com/>

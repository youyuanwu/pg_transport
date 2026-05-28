#### nupdate

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 2397 | 2365 | 0.99x | 6.03 | 7.71 | 1.28x |
| direct | 8 | 2262 | 2270 | 1.00x | 5.93 | 7.87 | 1.33x |

#### select

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 35742 | 35690 | 1.00x | 6.09 | 7.80 | 1.28x |
| direct | 8 | 36165 | 35834 | 0.99x | 5.89 | 7.99 | 1.35x |

#### tpcb

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 483 | 508 | 1.05x | 6.19 | 7.72 | 1.25x |
| direct | 8 | 499 | 525 | 1.05x | 5.96 | 7.59 | 1.27x |


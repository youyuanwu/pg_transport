#### nupdate

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 2960 | 2961 | 1.00x | 5.92 | 7.69 | 1.30x |
| direct | 8 | 2965 | 2950 | 1.00x | 5.93 | 7.81 | 1.32x |

#### select

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 35549 | 36002 | 1.01x | 6.83 | 7.97 | 1.17x |
| direct | 8 | 35829 | 35961 | 1.00x | 7.34 | 7.67 | 1.04x |

#### tpcb

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla initial-conn | pg_transport initial-conn | init-conn ratio |
|---|---:|---:|---:|---:|---:|---:|---:|
| spi | 8 | 574 | 630 | 1.10x | 5.91 | 7.82 | 1.32x |
| direct | 8 | 582 | 622 | 1.07x | 5.90 | 7.77 | 1.32x |


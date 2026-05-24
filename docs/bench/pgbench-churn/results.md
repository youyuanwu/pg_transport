#### select

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla avg conn time | pg_transport avg conn time | conn-time ratio | Vanilla avg latency | pg_transport avg latency |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| spi | 4 | 655 | 7126 | 10.88x | 3.54 | 0.35 | 0.10x | 6.11 | 0.56 |
| spi | 16 | 828 | 11951 | 14.43x | 11.60 | 0.81 | 0.07x | 19.33 | 1.34 |
| spi | 32 | 824 | 12129 | 14.72x | 24.69 | 1.77 | 0.07x | 38.83 | 2.64 |


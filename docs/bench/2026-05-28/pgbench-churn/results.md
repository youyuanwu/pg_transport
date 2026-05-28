#### select

| Backend | Clients | Vanilla tps | pg_transport tps | tps ratio | Vanilla avg conn time | pg_transport avg conn time | conn-time ratio | Vanilla avg latency | pg_transport avg latency |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| spi | 4 | 649 | 7150 | 11.02x | 3.60 | 0.34 | 0.10x | 6.16 | 0.56 |
| spi | 16 | 829 | 12016 | 14.50x | 11.62 | 0.80 | 0.07x | 19.30 | 1.33 |
| spi | 32 | 816 | 12356 | 15.14x | 25.07 | 1.73 | 0.07x | 39.21 | 2.59 |


#### oltp_point_select

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 22884.98 | 27467.14 | 1.200x |
| spi | 16 | 36367.87 | 45886.31 | 1.262x |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 22885.97 | 27594.00 | 1.206x |
| direct | 16 | 36345.42 | 45890.90 | 1.263x |
| direct | 32 | 34518.50 | 45104.75 | 1.307x |

#### oltp_read_only

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 768.51 | 1204.97 | 1.568x |
| spi | 16 | 1209.27 | 1809.50 | 1.496x |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 765.32 | 1183.42 | 1.546x |
| direct | 16 | 1173.27 | 1825.63 | 1.556x |
| direct | 32 | 861.10 | 1832.57 | 2.128x |

#### oltp_update_index

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 1225.16 | 1183.74 | 0.966x |
| spi | 16 | 4423.30 | 3276.82 | 0.741x |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 1229.39 | 1150.66 | 0.936x |
| direct | 16 | 4336.69 | 3316.48 | 0.765x |
| direct | 32 | NA | NA | NAx |


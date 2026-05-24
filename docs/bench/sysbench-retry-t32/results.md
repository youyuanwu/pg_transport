#### oltp_point_select

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 32 | 33948.42 | 44333.49 | 1.306x |
| direct | 32 | 33655.39 | 44160.81 | 1.312x |

#### oltp_read_only

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 32 | 839.62 | 1833.61 | 2.184x |
| direct | 32 | NA | NA | NAx |

#### oltp_update_index

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 32 | 6797.52 | 6090.39 | 0.896x |
| direct | 32 | NA | NA | NAx |


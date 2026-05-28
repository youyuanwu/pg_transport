#### oltp_point_select

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 16 | 36249.15 | 45695.28 | 1.261x |
| spi | 32 | 32936.22 | 43661.09 | 1.326x |
| direct | 16 | 36429.37 | 45841.43 | 1.258x |
| direct | 32 | 33300.80 | 44644.27 | 1.341x |

#### oltp_read_only

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 16 | 1112.35 | 1800.58 | 1.619x |
| spi | 32 | NA | NA | NAx |
| direct | 16 | 1076.26 | 1780.09 | 1.654x |
| direct | 32 | NA | NA | NAx |

#### oltp_update_index

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 16 | 4119.39 | 3234.33 | 0.785x |
| spi | 32 | NA | NA | NAx |
| direct | 16 | 4036.65 | 3208.50 | 0.795x |
| direct | 32 | NA | NA | NAx |


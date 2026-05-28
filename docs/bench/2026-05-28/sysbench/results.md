#### oltp_point_select

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 22592.69 | 27199.90 | 1.204x |
| spi | 16 | 35445.53 | 44633.92 | 1.259x |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 21971.49 | 27298.11 | 1.242x |
| direct | 16 | 36556.20 | 44882.06 | 1.228x |
| direct | 32 | 31596.13 | 43552.94 | 1.378x |

#### oltp_read_only

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 755.78 | 1156.01 | 1.530x |
| spi | 16 | NA | NA | NAx |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 765.26 | 1170.82 | 1.530x |
| direct | 16 | 1115.68 | 1763.16 | 1.580x |
| direct | 32 | 681.68 | 1803.93 | 2.646x |

#### oltp_update_index

| Backend | Threads | Vanilla tps | pg_transport tps | Ratio |
|---|---:|---:|---:|---:|
| spi | 4 | 1223.53 | 1173.56 | 0.959x |
| spi | 16 | NA | NA | NAx |
| spi | 32 | NA | NA | NAx |
| direct | 4 | 1168.76 | 1151.00 | 0.985x |
| direct | 16 | 4058.87 | 382.42 | 0.094x |
| direct | 32 | NA | NA | NAx |


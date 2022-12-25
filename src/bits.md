N |   MASK  | ENV       | TXN          | DB       | PUT       | DBI        | NODE    | PAGE     | MRESIZE |
--|---------|-----------|--------------|----------|-----------|------------|---------|----------|---------|
0 |0000 0001|ALLOC_RSRV |TXN_FINISHED  |          |           |DBI_DIRTY   |F_BIGDATA|P_BRANCH  |         |
1 |0000 0002|ALLOC_UNIMP|TXN_ERROR     |REVERSEKEY|F_SUBDATA  |DBI_STALE   |F_SUBDATA|P_LEAF    |         |
2 |0000 0004|ALLOC_COLSC|TXN_DIRTY     |DUPSORT   |           |DBI_FRESH   |F_DUPDATA|P_OVERFLOW|         |
3 |0000 0008|ALLOC_SSCAN|TXN_SPILLS    |INTEGERKEY|           |DBI_CREAT   |         |P_META    |         |
4 |0000 0010|ALLOC_FIFO |TXN_HAS_CHILD |DUPFIXED  |NOOVERWRITE|DBI_VALID   |         |P_BAD     |         |
5 |0000 0020|           |TXN_DRAINED_GC|INTEGERDUP|NODUPDATA  |DBI_USRVALID|         |P_LEAF2   |         |
6 |0000 0040|           |              |REVERSEDUP|CURRENT    |DBI_DUPDATA |         |P_SUBP    |         |
7 |0000 0080|           |              |          |ALLDUPS    |DBI_AUDITED |         |          |         |
8 |0000 0100| _MAY_MOVE |              |          |           |            |         |          | <=      |
9 |0000 0200| _MAY_UNMAP|              |          |           |            |         |          | <=      |
10|0000 0400|           |              |          |           |            |         |          |         |
11|0000 0800|           |              |          |           |            |         |          |         |
12|0000 1000|           |              |          |           |            |         |          |         |
13|0000 2000|VALIDATION |              |          |           |            |         |P_SPILLED |         |
14|0000 4000|NOSUBDIR   |              |          |           |            |         |P_LOOSE   |         |
15|0000 8000|           |              |DB_VALID  |           |            |         |P_FROZEN  |         |
16|0001 0000|SAFE_NOSYNC|TXN_NOSYNC    |          |RESERVE    |            |RESERVE  |          |         |
17|0002 0000|RDONLY     |TXN_RDONLY    |          |APPEND     |            |APPEND   |          | <=      |
18|0004 0000|NOMETASYNC |TXN_NOMETASYNC|CREATE    |APPENDDUP  |            |         |          |         |
19|0008 0000|WRITEMAP   |<=            |          |MULTIPLE   |            |         |          | <=      |
20|0010 0000|UTTERLY    |              |          |           |            |         |          | <=      |
21|0020 0000|NOTLS      |<=            |          |           |            |         |          |         |
22|0040 0000|EXCLUSIVE  |              |          |           |            |         |          |         |
23|0080 0000|NORDAHEAD  |              |          |           |            |         |          |         |
24|0100 0000|NOMEMINIT  |TXN_PREPARE   |          |           |            |         |          |         |
25|0200 0000|COALESCE   |              |          |           |            |         |          |         |
26|0400 0000|LIFORECLAIM|              |          |           |            |         |          |         |
27|0800 0000|PAGEPERTURB|              |          |           |            |         |          |         |
28|1000 0000|ENV_TXKEY  |TXN_TRY       |          |           |            |         |          |         |
29|2000 0000|ENV_ACTIVE |              |          |           |            |         |          |         |
30|4000 0000|ACCEDE     |SHRINK_ALLOWED|DB_ACCEDE |           |            |         |          |         |
31|8000 0000|FATAL_ERROR|              |          |           |            |         |          |         |

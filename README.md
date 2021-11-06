# try-pyspark-streaming

```
activate_kitchensink
```

In terminal 1:

```bash
nc -lk 9999
```

If port in use:

```
netstat -vanp tcp | grep 9999
kill <pid is second value to left of 0x0102>
```

In terminal 2:

```bash
spark-submit streaming.py localhost 9999
```

In terminal 1:
```bash
foo bar
bar baz
```

In terminal 2:

```bash
-------------------------------------------
Batch: 0
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
+----+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
| bar|    1|
| foo|    1|
+----+-----+

-------------------------------------------
Batch: 2
-------------------------------------------
+----+-----+
|word|count|
+----+-----+
| bar|    2|
| foo|    1|
| baz|    1|
+----+-----+
```

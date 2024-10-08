
Rồi mở terminal chạy

```bash
 hdfs dfs -rm -r /bai2_output | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.BaiTapMapreduce.Bai2.Bai2a /bai2 /bai2_output
```

```bash
 hdfs dfs -rm -r /bai2b_output | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.BaiTapMapreduce.Bai2.Bai2b /bai2 /bai2b_output
```

Chạy xong dô `http://localhost:9870/explorer.html#/bai2_output` là thấy file output


Rồi mở terminal chạy

```bash
 hdfs dfs -rm -r /bai1b_output | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.BaiTapMapreduce.Bai1.Bai1a /bai1 /bai1b_output
```

```bash
hdfs dfs -rm -r /bai1bcombiner_output | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.BaiTapMapreduce.Bai1.Bai1bCombiner /bai1 /bai1bcombiner_output;
```



Chạy xong dô `http://localhost:9870/explorer.html#/bai1_output` là thấy file output

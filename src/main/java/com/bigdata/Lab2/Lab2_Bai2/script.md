```bash
hdfs dfs -mkdir /Lab2/Bai2
hdfs dfs -mkdir /Lab2/Bai2/input
hdfs dfs -put ./song.txt /Lab2/Bai2/input
hdfs dfs -rm -r /Lab2/Bai2/output  | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.Lab2.Lab2_Bai2.Bai2 /Lab2/Bai2/input /Lab2/Bai2/output
```
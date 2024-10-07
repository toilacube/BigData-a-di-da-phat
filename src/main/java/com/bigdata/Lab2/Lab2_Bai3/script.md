```bash
hdfs dfs -mkdir /Lab2/Bai3a

hdfs dfs -mkdir /Lab2/Bai3a/input

hadoop jar target/Lab2-1.0-SNAPSHOT.jar com.bigdata.Lab2.Lab2_Bai3.Bai3_Runner /Lab2/Bai3a/input/retail.txt /Lab2/Bai3a/output

hdfs dfs -rm -r /Lab2/Bai3b/output | hadoop jar target/BigData-1.0-SNAPSHOT.jar com.bigdata.Lab2.Lab2_Bai3.Bai3b /Lab2/Bai3a/output /Lab2/Bai3b/output
```
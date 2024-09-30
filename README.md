Tải file data về rồi giải nén

```
https://drive.google.com/drive/folders/1H3Shd70zhglqYHlGHbDhYdtoLg-72Su5
```

Dô file vừa giải nén, có 1 đống folder bai1 bai2 ..., mở terminal lên để thêm mấy file input vào hdfs:

```
hdfs dfs -put *.csv /bai1
hdfs dfs -put *.csv /bai2
hdfs dfs -put *.csv /bai3
hdfs dfs -put *.csv /bai4
hdfs dfs -put *.csv /bai5
```

Mở Intellij, nhìn vào thanh công cụ bên phải có chữ m(maven), ấn dô nó

![](./img.png)

Rồi ấn dô cái khoanh đỏ, ấn `mvn clean`, rồi tiếp như vậy, ấy `mvn install` để tạo jar file ở `target/`

Rồi dô readme từng bài copy lệnh chạy là xong

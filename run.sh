/home/hadoop/spark-1.2.1-bin-hadoop1/bin/spark-submit --master spark://hadoop-server:7077 --class com.xy.lr.spark.yidaMain --executor-memory 10G --driver-memory 4G  hinet-1.0-SNAPSHOT.jar hdfs://hadoop-server:9000/user/hadoop/ hdfs://hadoop-server:9000/hi_index1 1 >> hinet.log

/home/hadoop/spark-1.2.1-bin-hadoop1/bin/spark-submit --master spark://hadoop-server:7077 --class com.xy.lr.spark.yidaMain --executor-memory 10G --driver-memory 4G  hinet-1.0-SNAPSHOT.jar hdfs://hadoop-server:9000/hi_index1 hdfs://hadoop-server:9000/hi_index2 2 >> hinet.log

/home/hadoop/spark-1.2.1-bin-hadoop1/bin/spark-submit --master spark://hadoop-server:7077 --class com.xy.lr.spark.yidaMain --executor-memory 10G --driver-memory 4G  hinet-1.0-SNAPSHOT.jar hdfs://hadoop-server:9000/hi_index2 hdfs://hadoop-server:9000/hi_index3 3 >> hinet.log

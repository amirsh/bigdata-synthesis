Bigdata synthesis
=================

Let's use synthesis techniques for big data applications.


Running DataFrame
=======
Run "sbt package", copy generate jar file to server and run "spark-submit --class DataFrameExample --master yarn-cluster /home/guliyev/bigdata-synthesis_2.11-0.1-SNAPSHOT.jar --num-executors 20"
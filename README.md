Bigdata synthesis
=================

Let's use synthesis techniques for big data applications.

Running Spark code
=======

Run "sbt package", copy generated jar file to server and run "spark-submit --class YOUR_CLASS_NAME --master yarn-cluster PATH_TO_JAR_FILE --num-executors 20"

Example paramters:
PATH_TO_JAR_FILE = /home/guliyev/bigdata-synthesis_2.11-0.1-SNAPSHOT.jar
YOUR_CLASS_NAME = ThetaInequiJoin or DataFrameInequiJoin

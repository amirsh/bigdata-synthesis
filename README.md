Bigdata synthesis
=================

Let's use synthesis techniques for big data applications.

Running Spark code
=======
Run "sbt package", copy generate jar file to server and run "spark-submit --class yourClassName --master yarn-cluster path_to_jar_file --num-executors 20"

Example paramters
path_to_jar_file = /home/guliyev/bigdata-synthesis_2.11-0.1-SNAPSHOT.jar
yourClassName = RDDRJoinS or DataFrameExample
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object DataFrameInequiJoin extends OrderOrderJoinBenchmark{
    val sc = new SparkContext(new SparkConf().setAppName("DataFrameInequiJoin"))
    def queryProcess(ordersRDD: RDD[First], orders2RDD: RDD[Second]): Unit = {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val orders = ordersRDD.toDF("O_ORDERKEY", "O_CUSTKEY")
      //orders.persist
      
      val orders2 = orders2RDD.toDF("O_ORDERKEY", "O_CUSTKEY")
      //orders2.persist
      
      orders.registerTempTable("orders")
      orders2.registerTempTable("orders2")
      
      val count = sqlContext.sql("SELECT count(*) FROM orders o1, orders2 o2 where o1.O_ORDERKEY > o2.O_ORDERKEY")
      println("Result : " + count.collect())
    }
}


object DataFrameEquiJoin extends LineitemOrderJoinBenchmark{
    val sc = new SparkContext(new SparkConf().setAppName("DataFrameEquiJoin"))
    def queryProcess(lineitemRDD: RDD[First], ordersRDD: RDD[Second]): Unit = {
      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._
      val orders = ordersRDD.toDF("O_ORDERKEY", "O_CUSTKEY")
      //orders.persist
      
      val lineitems = lineitemRDD.toDF("L_ORDERKEY", "L_LINENUMBER")
      //lineitems.persist
      
      orders.registerTempTable("orders")
      lineitems.registerTempTable("lineitems")
      
      val count = sqlContext.sql("SELECT count(*) FROM orders o, lineitems l where o.O_ORDERKEY = l.L_ORDERKEY")
      println("Result : " + count.collect())
    }
}

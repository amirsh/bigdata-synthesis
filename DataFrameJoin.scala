import org.apache.spark.{SparkConf, SparkContext}

object DataFrameJoin {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("Data Frame Join"))

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val orders = Utility.getOrdersRDD(sc, Utility.getRootPath + "order.tbl").toDF("O_ORDERKEY", "O_CUSTKEY"
        /*, "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"*/)
      orders.persist
      //orders.count
      //println("Orders size : " + orders.count)

      //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").toDF("L_ORDERKEY", /*"L_PARTKEY", "L_SUPPKEY",*/ "L_LINENUMBER"/*, "L_QUANTITY", 
      //              "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", 
      //              "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", 
      //              "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"*/)

      val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath + "order.tbl").toDF("O_ORDERKEY", "O_CUSTKEY")
      orders2.persist
      //orders2.count

      //val count = orders.join(lineitem, orders("O_ORDERKEY") === lineitem("L_ORDERKEY")).count()
      //val count = orders.join(orders2, orders("O_ORDERKEY") > orders2("O_ORDERKEY")).count()
      
      orders.registerTempTable("orders")
      orders2.registerTempTable("orders2")
      
      val count = sqlContext.sql("SELECT count(*) FROM orders o1, orders2 o2 where o1.O_ORDERKEY > o2.O_ORDERKEY")
      println("Result : " + count.collect())
    }
}

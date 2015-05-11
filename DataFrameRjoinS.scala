import org.apache.spark.{SparkConf, SparkContext}

object DataFrameExample {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("Data Frame RjoinS Example"))

      val sqlContext = new org.apache.spark.sql.SQLContext(sc)
      import sqlContext.implicits._

      val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl").toDF("O_ORDERKEY", "O_CUSTKEY"
        /*, "O_ORDERSTATUS", "O_TOTALPRICE", "O_ORDERDATE", "O_ORDERPRIORITY", "O_CLERK", "O_SHIPPRIORITY", "O_COMMENT"*/)

      val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl").toDF("L_ORDERKEY", /*"L_PARTKEY", "L_SUPPKEY",*/ "L_LINENUMBER"/*, "L_QUANTITY", 
                    "L_EXTENDEDPRICE", "L_DISCOUNT", "L_TAX", "L_RETURNFLAG", 
                    "L_LINESTATUS", "L_SHIPDATE", "L_COMMITDATE", "L_RECEIPTDATE", 
                    "L_SHIPINSTRUCT", "L_SHIPMODE", "L_COMMENT"*/)

      val count = orders.join(lineitem, orders("O_ORDERKEY") === lineitem("L_ORDERKEY")).count()

      println("Result : " + count)
    }
}

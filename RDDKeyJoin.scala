import org.apache.spark.{SparkConf, SparkContext}

object RDDRKeyJoin {
	def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("RDD (Key, Value) join Example"))

      val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl").map(o => (o.O_ORDERKEY, o))
      val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl").map(l => (l.L_ORDERKEY, l))

      val count = orders.join(lineitem)
      println("Result : "  +count)
  }
}
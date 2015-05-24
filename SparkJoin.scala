import org.apache.spark.{SparkConf, SparkContext}

object SparkJoin {
	def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("SparkJoin"))

      val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").map(o => (o.O_ORDERKEY, o))
      val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").map(l => (l.L_ORDERKEY, l))

      val count = orders.join(lineitem).count
      println("Result : "  +count)
  }
}
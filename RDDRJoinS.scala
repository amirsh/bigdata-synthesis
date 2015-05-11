import org.apache.spark.{SparkConf, SparkContext}


object RDDRJoinS {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("RDD RjoinS Example"))

      val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl").map(o => (o.O_ORDERKEY, o.O_CUSTKEY))
      val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl").map(l => (l.L_ORDERKEY, l.L_LINENUMBER))

	  val result = orders.collect.map(or => lineitem.flatMap(li => if (or._1 == li._1) List(or, li) else Nil).count)
      println("Result : " + result.sum)
    }
}

import org.apache.spark.{SparkConf, SparkContext}

class RDDRJoinS {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("RDD RjoinS Example"))

      val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl")
      val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl")

      orders.map(or => lineitem.flatMap(li => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil))
    }
}

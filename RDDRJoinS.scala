import org.apache.spark.{SparkConf, SparkContext}


object RDDRJoinS {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("RDD RjoinS Example"))

      val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl").map(o => (o.O_ORDERKEY, o.O_CUSTKEY))
      val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl").map(l => (l.L_ORDERKEY, l.L_LINENUMBER))


	  //val result = orders.flatMap(or => lineitem.flatMap(li => if (or._1 == li._1) List(or, li) else Nil))
	  // <console>:33: error: type mismatch;
	  // found   : org.apache.spark.rdd.RDD[(Int, Int)]
	  // required: TraversableOnce[?]
	  //             val result = orders.flatMap(or => lineitem.flatMap(li => if (or._1 == li._1) List(or, li) else Nil))
                            

	  //val result = orders.flatMap(or => lineitem.flatMap(li => if (or._1 == li._1) List(or, li) else Nil).collect)
	  // org.apache.spark.SparkException: Task not serializable


      // val result = orders.collectAsMap
      // val rdd_joined = lineitem.map({case (or, li) => ((or, li), result.get(or))})

      //println("Result : " + result.count)
    }
}

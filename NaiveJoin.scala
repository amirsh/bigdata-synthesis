import org.apache.spark.{SparkConf, SparkContext}


object NaiveJoin {

    def main(args: Array[String]) {
      val sc = new SparkContext(new SparkConf().setAppName("NaiveJoin"))

      val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").map(o => (o.O_ORDERKEY, o.O_CUSTKEY))
      //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").map(l => (l.L_ORDERKEY, l.L_LINENUMBER))
      val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").map(o => (o.O_ORDERKEY, o.O_CUSTKEY))

	  // val result = orders.collect.map(or => lineitem.flatMap(li => if (or._1 == li._1) List(or, li) else Nil).count)
	  // println("Result : " + result.sum)
	   val orderRam = orders.collect
	   // val result = lineitem.flatMap(li => orderRam.flatMap(or => if (or._1 == li._1) List(or, li) else Nil)).count
     val result = orderRam.map(or => orders2.flatMap(li => if (or._1 == li._1) List(or, li) else Nil).count).sum
      println("Result : " + result)
    }
}

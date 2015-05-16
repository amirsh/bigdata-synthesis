import org.apache.spark.{SparkConf, SparkContext}


// Broadcast LineItems
object RjoinSBroadcast {
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("R broadcast join S Example"))
		val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
      	val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")

  //     	val lineitemBroadcast = sc.broadcast(lineitem.map(l => (l.L_ORDERKEY, l)).collectAsMap)
		// val joined = orders.mapPartitions({ iter =>
		// 	val l = lineitemBroadcast.value
	 //  		for { 
	 //  			o <- iter
	 //  			if l.contains(o.O_ORDERKEY)
	 //    	} yield(o, l)
	 //    }, preservesPartitioning = true)
	 //    val count = joined.count

	 //    println("Result : "  +count)

	 	val lineitemBroadcast = sc.broadcast(lineitem.collect)
	 	val joined = orders.flatMap(or => lineitemBroadcast.value.flatMap(li => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
		val count = joined

		
		println("Result : " + count)
	}
}
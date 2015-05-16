import org.apache.spark.{SparkConf, SparkContext}


// Broadcast Orders
object RBroadcastJoinS {
	def main(args: Array[String]) {
		
		val sc = new SparkContext(new SparkConf().setAppName("R broadcast join S Example"))
		val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
	    val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")

	     // val ordersBroadcast = sc.broadcast(orders.map(o => (o.O_ORDERKEY, o)).collectAsMap)
		 // val joined = lineitem.mapPartitions({ iter =>
		 // 	val o = ordersBroadcast.value
		 //  		for { 
		 //  			l <- iter
		 //  			if o.contains(l.L_ORDERKEY)
		 //    	} yield(o, l)
		 //    }, preservesPartitioning = true)
			
	    // val ordersBroadcast = sc.broadcast(orders)
	    val ordersBroadcast = sc.broadcast(orders.collect)
		// val joined = lineitem.mapPartitions({ iter =>
		// 	val o = ordersBroadcast.value
		// 	for {
		// 		l <- iter
		// 		if (o.map(or => if (or.O_ORDERKEY == l.L_ORDERKEY) 1 else 0).sum > 0)
		// 	} yield (1)
		// 	// iter.flatMap(l => o.flatMap(or => if (or.O_ORDERKEY == l.L_ORDERKEY) List(or, l) else Nil).iterator)
		// 	// for { 
	 //  // 			l <- iter
	 //  // 			if o.exists(or => or.O_ORDERKEY == l.L_ORDERKEY)
	 //  //   	} yield(o, l)
		// }, preservesPartitioning = true)
		// val count = joined.sum
		val joined = lineitem.flatMap(li => ordersBroadcast.value.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
		val count = joined

		
		println("Result : " + count)
	}
}
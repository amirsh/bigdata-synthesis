import org.apache.spark.{SparkConf, SparkContext}


// Broadcast Orders
object BlockNestedJoin {
	def main(args: Array[String]) {
		
		val sc = new SparkContext(new SparkConf().setAppName("BlockNestedJoin K"))
		val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
	    //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")
		val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
	    
		var k = args(0).toInt
		val ordersAsList = orders.collect
		val len = ordersAsList.size / k
		var count: Double = 0;


		val ordersBlock = ordersAsList.grouped(len).toList
		for (block <- ordersBlock) {
			val ordersBroadcast = sc.broadcast(block)
			//val joined = lineitem.flatMap(li => ordersBroadcast.value.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
			val joined = orders2.map(li => ordersBroadcast.value.map(or => if (or.O_ORDERKEY > li.O_ORDERKEY) List(or, li) else Nil)).count
			count = count + joined;
		}

		// ordersAsList.foreach {

		// }

		// while(k > 0) {

		// 	var left: List[Order]
		// 	var ritght: List[Order]

		// 	if (k > 1) {
		// 		(left, right) = ordersAsList.splitAt(len);
		// 		ordersAsList = ordersAsList.drop(len);
		// 	} else {
		// 		left = ordersAsList;
		// 	}
			
		// 	val ordersBroadcast = sc.broadcast(left)
		// 	val joined = lineitem.flatMap(li => ordersBroadcast.value.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
			
		// 	count = count + joined
		// 	k = k - 1;
		// }
		
		println("Result : " + count)
	}
}
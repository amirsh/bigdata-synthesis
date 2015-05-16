import org.apache.spark.{SparkConf, SparkContext}


// Broadcast Orders
object BlockNestedJoin {
	def main(args: Array[String]) {
		
		val sc = new SparkContext(new SparkConf().setAppName("BlockNestedJoin Example"))
		val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
	    val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")
		
		var k = args(0).toInt
		val ordersAsList = orders.collect
		val len = ordersAsList.size / k
		var count: Long = 0;


		val ordersBlock = ordersAsList.grouped(len).toList
		for (block <- ordersBlock) {
			val joined = lineitem.flatMap(li => block.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
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
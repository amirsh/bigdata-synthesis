import org.apache.spark.{SparkConf, SparkContext}


// Broadcast Orders
object RBroadcastJoinS {
	def main(args: Array[String]) {
		
		val sc = new SparkContext(new SparkConf().setAppName("R broadcast join S Example"))
		val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl")
      	val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl")

      	val ordersBroadcast = sc.broadcast(orders.map(o => (o.O_ORDERKEY, o)).collectAsMap)
		val joined = lineitem.mapPartitions({ iter =>
			val o = ordersBroadcast.value
	  		for { 
	  			l <- iter
	  			if o.contains(l.L_ORDERKEY)
	    	} yield(o, l)
	    }, preservesPartitioning = true)

	    val count = joined.count
	    println("Result : "  +count)
	}
}
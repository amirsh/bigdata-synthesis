import org.apache.spark.{SparkConf, SparkContext}


// Broadcast LineItems
class RjoinSBroadcast {
	def main(args: Array[String]) {
		val sc = new SparkContext(new SparkConf().setAppName("R broadcast join S Example"))
		val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl")
      	val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl")

      	val lineitemBroadcast = sc.broadcast(lineitem.map(l => (l.L_ORDERKEY, l)).collectAsMap)
		val joined = orders.mapPartitions({ iter =>
			val l = lineitemBroadcast.value
	  		for { 
	  			o <- iter
	  			if l.contains(o.O_ORDERKEY)
	    	} yield(o, l)
	    }, preservesPartitioning = true)
	    joined.collect()
	}
}
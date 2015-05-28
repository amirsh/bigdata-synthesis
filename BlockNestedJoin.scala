import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


// Broadcast Orders
object BlockNestedInequiJoin extends OrderOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("BlockNestedInequiJoin K"))
	var k: Int = _
	override def preprocess(args: Array[String]): Unit = {
		k = args(0).toInt
	}
	def queryProcess(orders: RDD[First], orders2: RDD[Second]): Unit = {
		val ordersAsList = orders.collect
		val len = ordersAsList.size / k
		var count: Double = 0;


		val ordersBlock = ordersAsList.grouped(len).toList
		for (block <- ordersBlock) {
			val ordersBroadcast = sc.broadcast(block)
			//val joined = lineitem.flatMap(li => ordersBroadcast.value.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
			val joined = orders2.map(li => ordersBroadcast.value.map(or => if (or.O_ORDERKEY > li.O_ORDERKEY) List((or, li)) else Nil)).count
			count = count + joined;
		}
		println("Result : " + count)
	}
}

object BlockNestedEquiJoin extends LineitemOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("BlockNestedEquiJoin K"))
	var k: Int = _
	override def preprocess(args: Array[String]): Unit = {
		k = args(0).toInt
	}
	def queryProcess(lineitem: RDD[First], orders: RDD[Second]): Unit = {
		val ordersAsList = orders.collect
		val len = ordersAsList.size / k
		var count: Double = 0;


		val ordersBlock = ordersAsList.grouped(len).toList
		for (block <- ordersBlock) {
			val ordersBroadcast = sc.broadcast(block)
			val joined = lineitem.flatMap(li => ordersBroadcast.value.flatMap(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List((or, li)) else Nil)).count
			count = count + joined;
		}
		println("Result : " + count)
	}
}

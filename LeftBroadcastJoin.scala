import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object LeftBroadcastInequiJoin extends OrderOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("LeftBroadcastEquiJoin"))
	def queryProcess(orders: RDD[First], orders2: RDD[Second]): Unit = {
		val ordersBroadcast = sc.broadcast(orders.collect)
		val joined = orders2.map(li => ordersBroadcast.value.map(or => if (or.O_ORDERKEY > li.O_ORDERKEY) List(or, li) else Nil)).count
		println("Result : " + joined)
	}
}

object LeftBroadcastEquiJoin extends LineitemOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("LeftBroadcastEquiJoin"))
	def queryProcess(lineitem: RDD[First], orders: RDD[Second]): Unit = {
		val ordersBroadcast = sc.broadcast(orders.collect)
		val joined = lineitem.map(li => ordersBroadcast.value.map(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
		println("Result : " + joined)
	}
}
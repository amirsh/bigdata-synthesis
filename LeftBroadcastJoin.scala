import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object LeftBroadcastEquiJoin extends LineitemOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("LeftBroadcastEquiJoin"))
	def queryProcess(lineitem: RDD[First], orders: RDD[Second]): Unit = {
		val ordersBroadcast = sc.broadcast(orders.collect)
		val joined = lineitem.map(li => ordersBroadcast.value.map(or => if (or.O_ORDERKEY == li.L_ORDERKEY) List(or, li) else Nil)).count
		println("Result : " + joined)
	}
}
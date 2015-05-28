import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RightBroadEquiJoin extends LineitemOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("RightBroadEquiJoin"))
	def queryProcess(lineitem: RDD[First], orders: RDD[Second]): Unit = {
		val lineitemBroadcast = sc.broadcast(lineitem.collect)
	 	val joined = orders.flatMap(or => lineitemBroadcast.value.flatMap(li => if (or.O_ORDERKEY == li.L_ORDERKEY) List((or, li)) else Nil)).count
		println("Result : " + joined)
	}
}
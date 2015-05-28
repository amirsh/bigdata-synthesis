import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkJoin extends LineitemOrderJoinBenchmark {
	val sc = new SparkContext(new SparkConf().setAppName("SparkJoin"))

	def queryProcess(lineitemRDD: RDD[First], ordersRDD: RDD[Second]): Unit = {

      	val orders = ordersRDD.map(o => (o.O_ORDERKEY, o))
      	val lineitem = lineitemRDD.map(l => (l.L_ORDERKEY, l))
		
		val count = orders.join(lineitem).count
      	println("Result : "  +count)	
	}
}
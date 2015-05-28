import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object NaiveInequiJoin extends OrderOrderJoinBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("BlockNestedInequiJoin K"))
  def queryProcess(ordersRDD: RDD[First], orders2RDD: RDD[Second]): Unit = {

      val orders = ordersRDD.map(o => (o.O_ORDERKEY, o.O_CUSTKEY))
      val orders2 = orders2RDD.map(o => (o.O_ORDERKEY, o.O_CUSTKEY))

      val orderRam = orders.collect
      val result = orderRam.map(or => orders2.flatMap(li => if (or._1 > li._1) List(or, li) else Nil).count).sum
      println("Result : " + result)
  }
}

object NaiveEquiJoin extends LineitemOrderJoinBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("LeftBroadcastJoin"))
  def queryProcess(lineitemRDD: RDD[First], ordersRDD: RDD[Second]): Unit = {

      val orders = ordersRDD.map(o => (o.O_ORDERKEY, o.O_CUSTKEY))
      val orders2 = lineitemRDD.map(l => (l.L_ORDERKEY, l.L_LINENUMBER))

    val orderRam = orders.collect
    val result = orderRam.map(or => orders2.flatMap(li => if (or._1 == li._1) List(or, li) else Nil).count).sum
    println("Result : " + result)
  }
}
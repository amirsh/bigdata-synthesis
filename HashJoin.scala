import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object HashJoin extends LineitemOrderJoinBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("HashJoin"))
  def queryProcess(lineitemRDD: RDD[First], ordersRDD: RDD[Second]): Unit = {

    val orders = ordersRDD.map(o => (o.O_ORDERKEY, o))
    val lineitem = lineitemRDD.map(l => (l.L_ORDERKEY, l))

    val partOrders = orders.partitionBy(new org.apache.spark.HashPartitioner(200))
    val partLineitems = lineitem.partitionBy(new org.apache.spark.HashPartitioner(200))

    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList
      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._1 == li._1) List((or, li)) else Nil))
      localJoin.iterator
    })
    
    println("Result : " + zipOLI.count)
  }
}

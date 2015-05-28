import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD


object ScalaListInequiJoin extends OrderOrderJoinBenchmark {
    val sc = new SparkContext(new SparkConf().setAppName("ScalaListInequiJoin"))
    def queryProcess(ordersRDD: RDD[First], orders2RDD: RDD[Second]): Unit = {
        val orders = ordersRDD.collect.toList
        val orders2 = orders2RDD.collect.toList

        val join = orders.flatMap(or => orders2.flatMap(li => if (or.O_ORDERKEY > li.O_ORDERKEY) List((or, li)) else Nil))
        println("Result : " + join.size)
    }
}

object ScalaListEquiJoin extends LineitemOrderJoinBenchmark {
    val sc = new SparkContext(new SparkConf().setAppName("ScalaListEquiJoin"))
    def queryProcess(lineitemRDD: RDD[First], ordersRDD: RDD[Second]): Unit = {
        val orders = ordersRDD.collect.toList
        val lineitem = lineitemRDD.collect.toList
        val join = orders.flatMap(or => lineitem.flatMap(li => if (or.O_ORDERKEY == li.L_ORDERKEY) List((or, li)) else Nil))
        println("Result : " + join.size)
    
    }
}
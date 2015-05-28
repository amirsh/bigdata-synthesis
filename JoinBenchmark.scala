import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD

trait JoinBenchmark {
  val sc: SparkContext
  type First
  type Second
  def preprocess(args: Array[String]): Unit = {}
  def main(args: Array[String]) {
    preprocess(args)
    val (l, r) = load()
    queryProcess(l, r)
  }

  def load(): (RDD[First], RDD[Second])
  def queryProcess(l: RDD[First], r: RDD[Second]): Unit
}

trait LineitemOrderJoinBenchmark extends JoinBenchmark {
  type First = Lineitem
  type Second = Order

  def load(): (RDD[First], RDD[Second]) = {
    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
    val lineitem = Utility.getLineItemsRDD(sc, Utility.getRootPath+"lineitem.tbl")
    lineitem -> orders
  }
}

trait OrderOrderJoinBenchmark extends JoinBenchmark {
  type First = Order
  type Second = Order

  def load(): (RDD[First], RDD[Second]) = {
    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
    val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
    orders -> orders2
  }
}

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._
import org.apache.spark.rdd.RDD


class KeyPartitioner(partitions: Int) extends org.apache.spark.Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {    
    key.asInstanceOf[Int]
  }
}


object ThetaInequiJoin extends OrderOrderJoinBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("ThetaInequiJoin"))
  def queryProcess(orders: RDD[First], orders2: RDD[Second]): Unit = {
    val assignment = new ContentInsensitiveMatrixAssignment(orders.count(), orders2.count(), 256, 13)
    val partOrders = orders.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.ROW)) yield (i, r)).partitionBy(new KeyPartitioner(256))
    val partLineitems = orders2.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.COLUMN)) yield (i, r)).partitionBy(new KeyPartitioner(256))

    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList

      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._2.O_ORDERKEY > li._2.O_ORDERKEY) List((or, li)) else Nil))
      localJoin.iterator
    })

    println("SIZE is : " + zipOLI.count) 

  }
}

object ThetaEquiJoin extends LineitemOrderJoinBenchmark {
  val sc = new SparkContext(new SparkConf().setAppName("ThetaEquiJoin"))
  def queryProcess(lineitem: RDD[First], orders: RDD[Second]): Unit = {
    val assignment = new ContentInsensitiveMatrixAssignment(orders.count(), lineitem.count(), 256, 13)
    val partOrders = orders.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.ROW)) yield (i, r)).partitionBy(new KeyPartitioner(256))
    val partLineitems = lineitem.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.COLUMN)) yield (i, r)).partitionBy(new KeyPartitioner(256))

    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList

      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._2.O_ORDERKEY == li._2.L_ORDERKEY) List((or, li)) else Nil))
      localJoin.iterator
    })

    println("SIZE is : " + zipOLI.count) 
  }
}

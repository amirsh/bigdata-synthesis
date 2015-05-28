import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._

class KeyPartitioner(partitions: Int) extends org.apache.spark.Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {    
    key.asInstanceOf[Int]
  }
}

object ThetaJoin2 {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("ThetaJoin"))
    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
    //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")
    val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")

    val assignment = new ContentInsensitiveMatrixAssignment(orders.count(), orders2.count(), 32, 13)

    val partOrders = orders.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.ROW)) yield (i, r)).partitionBy(new KeyPartitioner(32))
    val partLineitems = orders2.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.COLUMN)) yield (i, r)).partitionBy(new KeyPartitioner(32))

    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList

      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._2.O_ORDERKEY > li._2.O_ORDERKEY) List(or._2) else Nil))
      //val localJoin = for ((ok, sum) <- lineitems if orders.contains(ok)) yield (orders(ok), sum)
      localJoin.iterator
    })

    println("SIZE is : " + zipOLI.count) 
  }
}



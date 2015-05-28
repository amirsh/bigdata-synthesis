import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions._

case class CustomKey(ORDERKEY: Int, regionId: Int)

object ThetaJoin {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("ThetaJoin"))
    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")
    //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl")
    val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl")


    val assignment = new ContentInsensitiveMatrixAssignment(orders.count(), orders2.count(), 256, 13)
    //val firstRelationRegions = assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.ROW)
    //val secondRelationRegions = assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.COLUMN)


    val partOrders = orders.flatMap(r => for (i <-assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.ROW)) yield (CustomKey(r.O_ORDERKEY, i), r.O_CUSTKEY)).partitionBy(new ExactPartitioner(256))
    //val partLineitems = lineitem.flatMap(r => for (i <- secondRelationRegions) yield (CustomKey(r.L_ORDERKEY, i), r.L_LINENUMBER)).partitionBy(new ExactPartitioner(256))
    val partLineitems = orders2.flatMap(r => for (i <- assignment.getRegionIDs(ContentInsensitiveMatrixAssignment.Dimension.COLUMN)) yield (CustomKey(r.O_ORDERKEY, i), r.O_CUSTKEY)).partitionBy(new ExactPartitioner(256))

    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList
      
      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._1.ORDERKEY > li._1.ORDERKEY) List(or, li) else Nil))

      //val localJoin = for ((ok, sum) <- lineitems if orders.contains(ok)) yield (orders(ok), sum)
      localJoin.iterator
    })

    val partResult2 = zipOLI.count

    println("SIZE is : " + partResult2)
  }


}



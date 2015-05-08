import org.apache.spark.{SparkConf, SparkContext}


// Partion Orders and Lineitesm on orderKey
// do local join
// collect data
class RlocalJoinS {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("Local Join Example"))

    val orders = Utility.getOrdersRDD(sc, "hdfs:///user/guliyev/sf1/orders.tbl").map(o => (o.O_ORDERKEY, o))
    val lineitem = Utility.getLineItemsRDD(sc,"hdfs:///user/guliyev/sf1/lineitem.tbl").map(l => (l.L_ORDERKEY, l))

    val partOrders = orders.partitionBy(new org.apache.spark.HashPartitioner(200))
    val partLineitems = lineitem.partitionBy(new org.apache.spark.HashPartitioner(200))

    partOrders.persist()
    partLineitems.persist()


    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toMap
      val lineitems = lineitems0.toList
      val localJoin =
        for ((orderkey, l) <- lineitems if orders.contains(orderkey)) yield (orders(orderkey), l)
      localJoin.iterator
    })
    val joinResult = zipOLI.collect
  }
}



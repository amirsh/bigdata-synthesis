import org.apache.spark.{SparkConf, SparkContext}

object HashJoin {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("HashJoin"))

    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").map(o => (o.O_ORDERKEY, o))
    val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").map(l => (l.L_ORDERKEY, l))

    val partOrders = orders.partitionBy(new org.apache.spark.HashPartitioner(200))
    val partLineitems = lineitem.partitionBy(new org.apache.spark.HashPartitioner(200))

    //partOrders.persist()
    //partLineitems.persist()


    val zipOLI = partOrders.zipPartitions(partLineitems)((orders0, lineitems0) => {
      val orders = orders0.toList
      val lineitems = lineitems0.toList
      // val localJoin =
        // for ((orderkey, l) <- lineitems if orders.contains(orderkey)) yield (orders(orderkey), l)
      val localJoin = orders.flatMap(or => lineitems.flatMap(li => if (or._1 == li._1) List(or, li) else Nil))
      localJoin.iterator
    })
    
    val count = zipOLI.count

    println("Result : " + count)
  }
}



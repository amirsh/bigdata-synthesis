import org.apache.spark.{SparkConf, SparkContext}

object ScalaListJoin {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("ScalaListJoin"))

    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").collect.toList
    //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").collect.toList
    val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").collect.toList

    val join = orders.flatMap(or => orders2.flatMap(li => if (or.O_ORDERKEY > li.O_ORDERKEY) List(or, li) else Nil))
  
    println("Result : " + join.size)
    // val list = scala.collection.mutable.ArrayBuffer[Order]()
    // orders.foreach(or =>
    // 	orders2.foreach(or2 =>
    // 		// if((or.O_ORDERKEY - or2.O_ORDERKEY) < 5 && (or.O_ORDERKEY - or2.O_ORDERKEY) > -5)
    // 			// count += 1
    // 			list += or
    // 		))
    // println(list.toList)
    // println(list.size)
  }
}

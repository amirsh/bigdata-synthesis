import org.apache.spark.{SparkConf, SparkContext}

object ScalaListJoin {
  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("ScalaListJoin"))

    val orders = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").collect.toList
    //val lineitem = Utility.getLineItemsRDD(sc,Utility.getRootPath+"lineitem.tbl").collect.toList
    val orders2 = Utility.getOrdersRDD(sc, Utility.getRootPath+"order.tbl").collect.toList

    val join = orders.flatMap(or => orders2.flatMap(li => if (or.O_ORDERKEY > li.O_ORDERKEY) List(or, li) else Nil))
  
    println("Result : " + join.size)
  }
}

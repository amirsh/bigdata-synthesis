import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

object Utility  {
	def getOrders(sc: SparkContext, path: String) = {
		    val orders = sc.textFile(path).flatMap(s => {
			      val fields = s.split("\\|")
			      val O_ORDERKEY = fields(0).toInt
			      val O_CUSTKEY = fields(1).toInt
			      val O_ORDERSTATUS = fields(2)
			      val O_TOTALPRICE = fields(3).toDouble
			      val O_ORDERDATE = fields(4)
			      val O_ORDERPRIORITY = fields(5)
			      val O_CLERK = fields(6)
			      val O_SHIPPRIORITY = fields(7)
			      val O_COMMENT = fields(8)
			      List((O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT))
			    })
		orders
	}

	def getLineItems(sc: SparkContext, path: String) = {
		val lineitem = sc.textFile(path).flatMap(s => {
	        val fields = s.split("\\|")
	        val L_ORDERKEY = fields(0).toInt
	        val L_PARTKEY = fields(1).toInt
	        val L_SUPPKEY = fields(2).toInt
	        val L_LINENUMBER = fields(3).toInt
	        val L_QUANTITY = fields(4).toDouble
	        val L_EXTENDEDPRICE = fields(5).toDouble
	        val L_DISCOUNT = fields(6).toDouble
	        val L_TAX = fields(7).toDouble
	        val L_RETURNFLAG = fields(8)
	        val L_LINESTATUS = fields(9)
	        val L_SHIPDATE = fields(10)
	        val L_COMMITDATE = fields(11)
	        val L_RECEIPTDATE = fields(12)
	        val L_SHIPINSTRUCT = fields(13)
	        val L_SHIPMODE = fields(14)
			val L_COMMENT = fields(15)

	        List((L_ORDERKEY, L_PARTKEY ,L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT))
	     })

		lineitem
	}
}
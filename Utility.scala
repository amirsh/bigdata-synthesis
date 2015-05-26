import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

case class Order(O_ORDERKEY: Int, 
					O_CUSTKEY: Int)
					//O_ORDERSTATUS: String, 
                 	//O_TOTALPRICE: Double, 
                    //O_ORDERDATE: String, 
                    //O_ORDERPRIORITY: String, 
                    //O_CLERK: String, 
                    //O_SHIPPRIORITY: String, 
                    //O_COMMENT: String)

case class Lineitem(L_ORDERKEY: Int, 
					//L_PARTKEY: Int, 
					//L_SUPPKEY: Int, 
					L_LINENUMBER: Int)//, 
					//L_QUANTITY: Double, 
                    //L_EXTENDEDPRICE: Double, 
                    //L_DISCOUNT: Double, 
                    //L_TAX: Double)
                    //L_RETURNFLAG: String, 
                    //L_LINESTATUS: String, 
                    //L_SHIPDATE: String, 
                    //L_COMMITDATE: String, 
                    //L_RECEIPTDATE: String, 
                    //L_SHIPINSTRUCT: String, 
                    //L_SHIPMODE: String, 
                    //L_COMMENT:String)

object Utility  {
	def getRootPath = {
		"hdfs:///user/guliyev/tiny/"
	}

	def getOrdersRDD(sc: SparkContext, path: String) = {
		    val orders = sc.textFile(path).map(s => {
			      val fields = s.split("\\|")
			      val O_ORDERKEY = fields(0).toInt
			      val O_CUSTKEY = fields(1).toInt
			      //val O_ORDERSTATUS = fields(2)
			      //val O_TOTALPRICE = fields(3).toDouble
			      //val O_ORDERDATE = fields(4)
			      //val O_ORDERPRIORITY = fields(5)
			      //val O_CLERK = fields(6)
			      //val O_SHIPPRIORITY = fields(7)
			      //val O_COMMENT = fields(8)
			      Order(O_ORDERKEY, O_CUSTKEY/*, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT*/)
			    })
		orders
	}

	def getLineItemsRDD(sc: SparkContext, path: String) = {
		val lineitem = sc.textFile(path).map(s => {
	        val fields = s.split("\\|")
	        val L_ORDERKEY = fields(0).toInt
	        //val L_PARTKEY = fields(1).toInt
	        //val L_SUPPKEY = fields(2).toInt
	        val L_LINENUMBER = fields(3).toInt
	        ///val L_QUANTITY = fields(4).toDouble
	        //val L_EXTENDEDPRICE = fields(5).toDouble
	        //val L_DISCOUNT = fields(6).toDouble
	        //val L_TAX = fields(7).toDouble
	        //val L_RETURNFLAG = fields(8)
	        //val L_LINESTATUS = fields(9)
	        //val L_SHIPDATE = fields(10)
	        //val L_COMMITDATE = fields(11)
	        //val L_RECEIPTDATE = fields(12)
	        //val L_SHIPINSTRUCT = fields(13)
	        //val L_SHIPMODE = fields(14)
			//val L_COMMENT = fields(15)
	        Lineitem(L_ORDERKEY, /*L_PARTKEY ,L_SUPPKEY,*/ L_LINENUMBER/*, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT*/)
	     })
		lineitem
	}
}
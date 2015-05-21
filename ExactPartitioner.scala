/**
 * Created by khayyam on 4/28/15.
 */

class ExactPartitioner(partitions: Int) extends org.apache.spark.Partitioner {

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = {
    val k = key.asInstanceOf[CustomKey]

    return k.regionId
  }
}

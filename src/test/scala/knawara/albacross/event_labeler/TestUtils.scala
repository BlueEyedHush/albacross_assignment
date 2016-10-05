package knawara.albacross.event_labeler

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object TestUtils {
  def getSqlContext(): SQLContext = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("event_labeler_test")
    val sparkContext = new SparkContext(sparkConf)
    new SQLContext(sparkContext)
  }

  def toBitString(b: Byte) = {
    val builder = new StringBuilder
    var byteTmp = b.asInstanceOf[Int]
    for(i <- 0 until 8) {
      builder.append(if((byteTmp & 0x80) > 0) 1 else 0)
      byteTmp = byteTmp << 1
    }
    builder.toString()
  }
}

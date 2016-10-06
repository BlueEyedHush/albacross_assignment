package knawara.albacross.event_labeler

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object TestUtils {
  val IP_COLUMN_NAME = "source_ip"
  val COMPANY_ID_NAME = "company_id"
  val RANGE_START_NAME = "ip_range_start"
  val RANGE_END_NAME = "ip_range_end"

  val EVENTS_SCHEMA = StructType(Seq(StructField(IP_COLUMN_NAME, StringType, false)))
  val MAPPING_SCHEMA = StructType(Seq(
    StructField(COMPANY_ID_NAME, LongType, false),
    StructField(RANGE_START_NAME, StringType, false),
    StructField(RANGE_END_NAME, StringType, false)
  ))

  private val contextLock = new ReentrantLock()
  private var context: SQLContext = null
  def getSqlContext(): SQLContext = {
    contextLock.lock()

    try {
      if(context == null) {
        val sparkConf = new SparkConf()
          .setMaster("local[*]")
          .setAppName("event_labeler_test")
        val sparkContext = new SparkContext(sparkConf)
        context = new SQLContext(sparkContext)
      }

      context
    } finally {
      contextLock.unlock()
    }
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

  def ipToByteArray(ip: String) = InetAddress.getByName(ip).getAddress
}

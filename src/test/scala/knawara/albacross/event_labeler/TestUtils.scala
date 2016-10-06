package knawara.albacross.event_labeler

import java.net.InetAddress

import knawara.albacross.event_labeler.types.{CompanyIdToIpRangeMapping, EventList}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object TestUtils {
  import CompanyIdToIpRangeMapping._
  import EventList._

  val EVENTS_SCHEMA = StructType(Seq(StructField(IP_COLUMN_NAME, StringType, false)))
  val MAPPING_SCHEMA = StructType(Seq(
    StructField(COMPANY_ID_NAME, LongType, false),
    StructField(RANGE_START_NAME, StringType, false),
    StructField(RANGE_END_NAME, StringType, false)
  ))

  def getSqlContext(): SQLContext = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("event_labeler_test")
      .set("spark.driver.allowMultipleContexts", "true")
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

  def ipToByteArray(ip: String) = InetAddress.getByName(ip).getAddress
}

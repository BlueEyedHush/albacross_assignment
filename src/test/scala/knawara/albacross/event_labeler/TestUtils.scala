package knawara.albacross.event_labeler

import java.net.InetAddress

import knawara.albacross.event_labeler.types.{EventList, CompanyIdToIpRangeMapping}
import org.apache.spark.sql.types._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object TestUtils {
  import EventList._
  import CompanyIdToIpRangeMapping._

  private val byteArraySchema = ArrayType(ByteType, false)
  val EVENTS_SCHEMA = StructType(Seq(StructField(ORIGINAL_IP_COLUMN_NAME, byteArraySchema, false)))
  val MAPPING_SCHEMA = StructType(Seq(
    StructField(COMPANY_ID_NAME, LongType, false),
    StructField(RANGE_START_NAME, byteArraySchema, false),
    StructField(RANGE_END_NAME, byteArraySchema, false)
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

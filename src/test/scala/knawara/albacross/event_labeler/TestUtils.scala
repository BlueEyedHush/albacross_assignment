package knawara.albacross.event_labeler

import java.net.InetAddress
import java.util.concurrent.locks.ReentrantLock

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object TestUtils {
  val IP_COLUMN_NAME = "source_ip"
  val COMPANY_ID_NAME = "company_id"
  val COMPANY_PRIORITY_NAME = "company_priority"
  val RANGE_START_NAME = "ip_range_start"
  val RANGE_END_NAME = "ip_range_end"

  val EVENTS_SCHEMA = StructType(Seq(StructField(IP_COLUMN_NAME, StringType, false)))
  val MAPPING_SCHEMA = StructType(Seq(
    StructField(COMPANY_ID_NAME, LongType, false),
    StructField(COMPANY_PRIORITY_NAME, LongType, false),
    StructField(RANGE_START_NAME, StringType, false),
    StructField(RANGE_END_NAME, StringType, false)
  ))

  private val contextLock = new ReentrantLock()
  private var context: SQLContext = null
  def getSqlContext(): SQLContext = {
    contextLock.lock()

    try {
      if(context == null) {
        import org.apache.log4j.Level
        Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

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
}

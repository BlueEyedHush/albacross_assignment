package knawara.albacross.event_labeler

import java.nio.ByteBuffer
import java.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, RowFactory, Row, DataFrame}
import org.scalatest.{FlatSpec, Matchers}

class EventListTests extends FlatSpec with Matchers {
  val sqlContext = TestUtils.getSqlContext()

  /**
    * generates all possible combinations of a 2B bytestring and then tests if they'll
    * be sorted correctly
    */
  "EventList" should "be sorted consistentnly with natural byte-string order" in {
    val unsortedEventListDf = EventList(EventListTests.buildEventListDf(sqlContext)).df
    val sortedDf = unsortedEventListDf.sort(unsortedEventListDf(EventList.GENERATED_IP_COLUMN_NAME))
    val bytesArray = sortedDf.collect()

    val expectedArray = for(i <- 0x0 to 0xff; j <- 0x0 to 0xff) yield (i.asInstanceOf[Byte],j.asInstanceOf[Byte])
    bytesArray.zip(expectedArray).foreach({case (actual, expected) => {
      val ba = actual.getAs[Seq[Byte]](EventList.ORIGINAL_IP_COLUMN_NAME)
      val str = actual.getAs[String](EventList.GENERATED_IP_COLUMN_NAME)

      if(ba(0) != expected._1 || ba(1) != expected._2) {
        import TestUtils.{toBitString => pb}
        println(s"expected ${pb(expected._1)} ${pb(expected._2)}, got ${pb(ba(0))} ${pb(ba(1))}; which is $str")
      }
    }})
  }
}

object EventListTests {
  private def buildEventListDf(sqlContext: SQLContext): DataFrame = {
    val rowList = new util.ArrayList[Row](256*256)
    for(i <- 0x0 to 0xff; j <- 0x0 to 0xff) {
      val byteArray = ByteBuffer.allocate(2)
        .put(i.asInstanceOf[Byte])
        .put(j.asInstanceOf[Byte])
        .array()

      rowList.add(RowFactory.create(byteArray))
    }

    val schema = StructType(Seq(StructField("source_ip", ArrayType(ByteType, false), false)))

    sqlContext.createDataFrame(rowList, schema)
  }
}

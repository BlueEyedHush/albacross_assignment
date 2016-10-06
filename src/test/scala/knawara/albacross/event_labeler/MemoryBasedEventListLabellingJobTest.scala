package knawara.albacross.event_labeler

import java.util

import knawara.albacross.event_labeler.types.{CompanyIdToIpRangeMapping, EventList}
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import org.scalatest._

/**
  * Test of EventListLabellingJob which uses memory-backed dataframes as input
  */
class MemoryBasedEventListLabellingJobTest extends FlatSpec with Matchers {
  import MemoryBasedEventListLabellingJobTest._

  val sqlContext = TestUtils.getSqlContext()

  "EventListLabellingJob" should "return correctly mapped dataset in non-overlapping case" in {
    val ips = Seq("1::5", "3::3", "5::5")
    val ranges = Seq(("1::0", "2::0"), ("3::0", "4::0"), ("5::0", "6::0"))

    val el = createEventList(sqlContext, ips)
    val m = createMapping(sqlContext, ranges)
    val job = new EventListLabelingJob(el, m)

    val result = job.run(sqlContext)

    /* TODO: ignore order */
    dfToCompanyIdSeq(result) should be (Seq(0,1,2))
  }
}

object MemoryBasedEventListLabellingJobTest {

  private def createEventList(sc: SQLContext, ips: Seq[String]): EventList = {
    val rowList = new util.ArrayList[Row](4)
    ips.foreach(ip => rowList.add(RowFactory.create(ip)))
    val df = sc.createDataFrame(rowList, TestUtils.EVENTS_SCHEMA)
    EventList(df)
  }

  private def createMapping(sc: SQLContext, ranges: Seq[(String, String)]): CompanyIdToIpRangeMapping = {
    val rowList = new util.ArrayList[Row](ranges.size)

    for(i <- 0 until ranges.size) {
      val (start, end) = ranges(i)
      val rowTuple = (i.asInstanceOf[Long], start, end)
      rowList.add(Row.fromTuple(rowTuple))
    }

    val df = sc.createDataFrame(rowList, TestUtils.MAPPING_SCHEMA)
    CompanyIdToIpRangeMapping(df)
  }

  private def dfToCompanyIdSeq(df: DataFrame): Seq[Long] = {
    df.collect().map(_.getAs[Long](CompanyIdToIpRangeMapping.COMPANY_ID_NAME))
  }
}

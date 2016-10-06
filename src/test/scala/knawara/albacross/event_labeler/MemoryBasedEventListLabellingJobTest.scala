package knawara.albacross.event_labeler

import java.util

import knawara.albacross.event_labeler.types.IpProcessor
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

    val el = createEventsDataset(sqlContext, ips)
    val m = createMappingDataset(sqlContext, ranges)
    val result = EventListLabelingJob(el, m).run(sqlContext)

    val expected = expandIps(ips).zipWithIndex.toSet
    dfToCompanyIdSet(result) should be (expected)
  }
}

object MemoryBasedEventListLabellingJobTest {
  import TestUtils._

  private def createEventsDataset(sc: SQLContext, ips: Seq[String]): EventsDataset = {
    val rowList = new util.ArrayList[Row](4)
    ips.foreach(ip => rowList.add(RowFactory.create(ip)))
    val df = sc.createDataFrame(rowList, TestUtils.EVENTS_SCHEMA)
    new EventsDataset(df, IP_COLUMN_NAME, "0")
  }

  private def createMappingDataset(sc: SQLContext, ranges: Seq[(String, String)]): MappingDataset = {
    val rowList = new util.ArrayList[Row](ranges.size)

    for(i <- 0 until ranges.size) {
      val (start, end) = ranges(i)
      val rowTuple = (i.asInstanceOf[Long], start, end)
      rowList.add(Row.fromTuple(rowTuple))
    }

    val df = sc.createDataFrame(rowList, MAPPING_SCHEMA)
    new MappingDataset(df, RANGE_START_NAME, RANGE_END_NAME)
  }

  private def dfToCompanyIdSet(df: DataFrame): Set[(String, Int)] = {
    df.collect().map(r => {
      val companyId = r.getAs[Long](COMPANY_ID_NAME).toInt
      val sourceIp = r.getAs[String](IP_COLUMN_NAME)
      (sourceIp, companyId)
    }).toSet
  }

  private def expandIps(ips: Seq[String]): Seq[String] = ips.map(IpProcessor.convertIpAddress)
}

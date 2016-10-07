package knawara.albacross.event_labeler

import java.util

import knawara.albacross.event_labeler.types.IpProcessor
import org.apache.spark.sql.{DataFrame, Row, RowFactory, SQLContext}
import org.scalatest._

/**
  * Test of EventListLabellingJob which uses memory-backed dataframes as input
  */
class EventLabelerTest extends FlatSpec with Matchers {
  import EventLabelerTest._

  val sqlContext = TestUtils.getSqlContext()

  "EventLabeler" should "correctly label events in non-overlapping case" in {
    val ips = Seq("1::5", "3::3", "5::5")
    val ranges = Seq((1L, "1::0", "2::0"), (2L, "3::0", "4::0"), (3L, "5::0", "6::0"))
    val expected = Seq(0, 1, 2)

    test(ips, ranges, expected)
  }

  it should "correctly label events in simple overlapping case" in {
    val ips = Seq("1::5", "3::3")
    val ranges = Seq((1L, "1::0", "5::0"), (2L, "1::0", "5::0"))
    val expected = Seq(0, 0)

    test(ips, ranges, expected)
  }

  it should "correctly label events in complex overlapping case" in {
    val ips = Seq("1::5", "2::2", "3::3", "::6:0:1", "::6:4:5", "::6:7:0")
    val ranges = Seq((2L, "1::0", "3::0"), (1L, "2::0", "4::0"), (4L, "::6:0:0", "::7:0:0"), (3L, "::6:4:0", "::6:5:0"))
    val expected = Seq(0, 1, 1, 2, 3, 2)

    test(ips, ranges, expected)
  }

  it should "correctly label events in case of single-point overlapping ranges" in {
    val ips = Seq("::2", "::3")
    val ranges = Seq((2L, "::0", "::2"), (3L, "::2", "::3"), (1L, "::3", "::4"))
    val expected = Seq(0, 2)

    test(ips, ranges, expected)
  }

  private def test(ips: Seq[String], ranges: Seq[(Long, String,String)], expectedCompanyIds: Seq[Int]) = {
    val el = createEventsDataset(sqlContext, ips)
    val m = createMappingDataset(sqlContext, ranges)
    val result = EventLabeler(el, m).run(sqlContext)

    val expected = expandIps(ips).zip(expectedCompanyIds).toSet
    dfToCompanyIdSet(result) should be (expected)
  }
}

object EventLabelerTest {
  import TestUtils._

  private def createEventsDataset(sc: SQLContext, ips: Seq[String]): EventsDataset = {
    val rowList = new util.ArrayList[Row](4)
    ips.foreach(ip => rowList.add(RowFactory.create(ip)))
    val df = sc.createDataFrame(rowList, TestUtils.EVENTS_SCHEMA)
    new EventsDataset(df, IP_COLUMN_NAME)
  }

  private def createMappingDataset(sc: SQLContext, ranges: Seq[(Long, String, String)]): MappingDataset = {
    val rowList = new util.ArrayList[Row](ranges.size)

    for(i <- ranges.indices) {
      val (priority, start, end) = ranges(i)
      val rowTuple = (i.asInstanceOf[Long], priority, start, end)
      rowList.add(Row.fromTuple(rowTuple))
    }

    val df = sc.createDataFrame(rowList, MAPPING_SCHEMA)
    new MappingDataset(df, COMPANY_ID_NAME, COMPANY_PRIORITY_NAME, RANGE_START_NAME, RANGE_END_NAME)
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

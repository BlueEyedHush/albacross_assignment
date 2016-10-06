package knawara.albacross.event_labeler

import knawara.albacross.event_labeler.types.IpProcessor
import org.apache.spark
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, Encoders, DataFrame, SQLContext}

class EventsDataset(var df: DataFrame, val ip: String)
class MappingDataset(var df: DataFrame, val company_id: String, val priority: String, val ipRangeStart: String, val ipRangeEnd: String)

class EventListLabelingJob private(val events: EventsDataset, val ranges: MappingDataset) {
  def run(sqlContext: SQLContext): DataFrame = {
    val eventTable = "events"
    val rangesTable = "ranges"

    val srcIp = s"$eventTable.${events.ip}"
    val rangeStart = s"$rangesTable.${ranges.ipRangeStart}"
    val rangeEnd = s"$rangesTable.${ranges.ipRangeEnd}"

    events.df.registerTempTable(eventTable)
    ranges.df.registerTempTable(rangesTable)

    import sqlContext.implicits._
    val resultSchema = events.df.schema.add(ranges.df.schema(ranges.company_id))
    implicit val resultEncoder = RowEncoder(resultSchema)

    val keyExtractor = SerializableFunctions.createKeyExtractor(events.ip)
    val reducer = SerializableFunctions.createReducer(ranges.priority)
    val df = sqlContext.sql(s"SELECT * FROM " +
      s"$eventTable LEFT OUTER JOIN $rangesTable ON $srcIp >= $rangeStart AND $srcIp <= $rangeEnd ")
      .groupByKey(keyExtractor)
      .reduceGroups(reducer)
      .map { case (k, row) => row }

    df.show(false)

    df
  }
}

object SerializableFunctions {
  def createKeyExtractor(ipColumnName: String): Row => String =
    r => r.getAs[String](ipColumnName)


  def createReducer(priorityColumnName: String): (Row, Row) => Row =
    (r1, r2) => {
      val p1 = r1.getAs[Long](priorityColumnName)
      val p2 = r2.getAs[Long](priorityColumnName)

      if (p1 < p2) r1 else r2
    }
}

object EventListLabelingJob {
  /**
    * Both datasets (events & ranges) must contain String columns holding IP addresses in IPv6 format
    * (not necessarily fully expanded)
    * They are stored in columns which names are also provided inside events & ranges objects
    *
    * This function validates them, processes to suitable format and returns EventListLabelingJob
    */
  def apply(events: EventsDataset, ranges: MappingDataset): EventListLabelingJob = {
    processRanges(ranges)
    processEvents(events)
    new EventListLabelingJob(events, ranges)
  }

  private def processRanges(ranges: MappingDataset): Unit = {
    ranges.df = IpProcessor.addIpFormattingStep(ranges.df, ranges.ipRangeStart)
    ranges.df = IpProcessor.addIpFormattingStep(ranges.df, ranges.ipRangeEnd)
  }

  private def processEvents(events: EventsDataset): Unit = {
    events.df = IpProcessor.addIpFormattingStep(events.df, events.ip)
  }
}

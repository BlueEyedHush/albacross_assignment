package knawara.albacross.event_labeler

import knawara.albacross.event_labeler.types.IpProcessor
import org.apache.spark.sql.{DataFrame, SQLContext}

class EventsDataset(var df: DataFrame, val ip: String, val priority: String)
class MappingDataset(var df: DataFrame, val ipRangeStart: String, val ipRangeEnd: String)

class EventListLabelingJob private (val events: EventsDataset, val ranges: MappingDataset) {
  def run(sqlContext: SQLContext): DataFrame = {
    val eventTable = "events"
    val rangesTable = "ranges"

    val srcIp = s"$eventTable.${events.ip}"
    val rangeStart = s"$rangesTable.${ranges.ipRangeStart}"
    val rangeEnd = s"$rangesTable.${ranges.ipRangeEnd}"

    events.df.registerTempTable(eventTable)
    ranges.df.registerTempTable(rangesTable)

    val df = sqlContext.sql(s"SELECT * FROM " +
      s"$eventTable LEFT OUTER JOIN $rangesTable ON $srcIp >= $rangeStart AND $srcIp <= $rangeEnd ")

    df.show(false)

    df
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

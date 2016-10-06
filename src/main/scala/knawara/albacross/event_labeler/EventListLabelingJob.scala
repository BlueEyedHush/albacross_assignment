package knawara.albacross.event_labeler

import knawara.albacross.event_labeler.types.{CompanyIdToIpRangeMapping, EventList}
import org.apache.spark.sql.{DataFrame, SQLContext}

class EventListLabelingJob(private val eventList: EventList,
                           private val mapping: CompanyIdToIpRangeMapping) {
  def run(sqlContext: SQLContext): DataFrame = {
    val eventTable = "events"
    val rangesTable = "ranges"

    import CompanyIdToIpRangeMapping._
    import EventList._
    val srcIp = s"$eventTable.$IP_COLUMN_NAME"
    val rangeStart = s"$rangesTable.$RANGE_START_NAME"
    val rangeEnd = s"$rangesTable.$RANGE_END_NAME"

    eventList.df.registerTempTable(eventTable)
    mapping.df.registerTempTable(rangesTable)

    val df = sqlContext.sql(s"SELECT * FROM " +
      s"$eventTable LEFT OUTER JOIN $rangesTable ON $srcIp >= $rangeStart AND $srcIp <= $rangeEnd ")

    df.show(false)

    df
  }
}

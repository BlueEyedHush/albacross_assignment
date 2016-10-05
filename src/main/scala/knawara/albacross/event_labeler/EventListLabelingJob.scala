package knawara.albacross.event_labeler

import knawara.albacross.event_labeler.types.{EventList, CompanyIdToIpRangeMapping}
import org.apache.spark.sql.{DataFrame, SQLContext}

class EventListLabelingJob(private val eventList: EventList,
                           private val mapping: CompanyIdToIpRangeMapping) {
  def run(sqlContext: SQLContext): DataFrame = {
    val eventTable = "events"
    val rangesTable = "ranges"

    import CompanyIdToIpRangeMapping._
    import EventList._
    val srcIp = s"$eventTable.$GENERATED_IP_COLUMN_NAME"
    val rangeStart = s"$rangesTable.$TRANSFORMED_RANGE_START_NAME"
    val rangeEnd = s"$rangesTable.$TRANSFORMED_RANGE_END_NAME"

    eventList.df.registerTempTable(eventTable)
    mapping.df.registerTempTable(rangesTable)

    val df = sqlContext.sql(s"SELECT * FROM " +
      s"$eventTable LEFT OUTER JOIN $rangesTable ON $srcIp >= $rangeStart AND $srcIp <= $rangeEnd ")

    df.show(false)

    df
  }
}

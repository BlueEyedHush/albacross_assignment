package knawara.albacross.event_labeler

import knawara.albacross.event_labeler.types.IpProcessor
import org.apache.spark
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{Row, Encoders, DataFrame, SQLContext}

class EventsDataset(var df: DataFrame,
                    val ip: String)
class MappingDataset(var df: DataFrame,
                     val company_id: String,
                     val priority: String,
                     val ipRangeStart: String,
                     val ipRangeEnd: String)

class EventLabeler private(val events: EventsDataset, val ranges: MappingDataset) {
  def run(sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val resultSchema = events.df.schema.add(ranges.df.schema(ranges.company_id))
    implicit val resultEncoder = RowEncoder(resultSchema)

    val keyExtractor = TransformationProducers.createKeyExtractor(events.ip)
    val reducer = TransformationProducers.createReducer(ranges.priority)
    val valueExtractor = TransformationProducers.createValueExtractor()

    val df = events.df
      .join(ranges.df, events.df(events.ip).between(ranges.df(ranges.ipRangeStart), ranges.df(ranges.ipRangeEnd)))
      .groupByKey(keyExtractor)
      .reduceGroups(reducer)
      .map(valueExtractor)

    df.show(false)

    df
  }
}

/**
  * Functions used as argument to Dataset operations are implemented here
  * so that during serialization Spark doesn't need to serialize whole EventLabeler
  */
object TransformationProducers {
  def createKeyExtractor(ipColumnName: String): Row => String =
    r => r.getAs[String](ipColumnName)


  def createReducer(priorityColumnName: String): (Row, Row) => Row =
    (r1, r2) => {
      val p1 = r1.getAs[Long](priorityColumnName)
      val p2 = r2.getAs[Long](priorityColumnName)

      if (p1 < p2) r1 else r2
    }

  def createValueExtractor(): ((String, Row)) => Row = { case (k, row) => row }
}

object EventLabeler {
  /**
    * Both datasets (events & ranges) must contain String columns holding IP addresses in IPv6 format
    * (not necessarily fully expanded)
    * They are stored in columns which names are also provided inside events & ranges objects
    *
    * This function validates them, processes to suitable format and returns EventLabeler
    */
  def apply(events: EventsDataset, ranges: MappingDataset): EventLabeler = {
    processRanges(ranges)
    processEvents(events)
    new EventLabeler(events, ranges)
  }

  private def processRanges(ranges: MappingDataset): Unit = {
    ranges.df = IpProcessor.addIpFormattingStep(ranges.df, ranges.ipRangeStart)
    ranges.df = IpProcessor.addIpFormattingStep(ranges.df, ranges.ipRangeEnd)
  }

  private def processEvents(events: EventsDataset): Unit = {
    events.df = IpProcessor.addIpFormattingStep(events.df, events.ip)
  }
}

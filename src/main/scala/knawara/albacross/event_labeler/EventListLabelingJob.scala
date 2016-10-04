package knawara.albacross.event_labeler

import org.apache.spark.sql.SQLContext

class EventListLabelingJob(private val eventList: EventList,
                           private val mapping: CompanyIdToIpRangeMapping) extends SparkJob {
  override def run(sqlContext: SQLContext): Unit = ???
}

package knawara.albacross.event_labeler

import org.apache.spark.sql.DataFrame

/**
  * This class validates the schema of provided dataframe, so that EventListLabellingJob doesn't need
  * to concern itself with that
  */
class EventList(val dataFrame: DataFrame) {

}

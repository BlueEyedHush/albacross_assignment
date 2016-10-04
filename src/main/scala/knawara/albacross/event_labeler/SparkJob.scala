package knawara.albacross.event_labeler

import org.apache.spark.sql.SQLContext

/**
  * Base class for jobs which don't want to concern themselves with Spark setup
  */
abstract class SparkJob {
  def run(sqLContext: SQLContext)
}

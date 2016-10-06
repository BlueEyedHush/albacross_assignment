package knawara.albacross.event_labeler.types

import org.apache.spark.sql.DataFrame

/**
  * This class validates the schema of provided dataframe, so that EventListLabellingJob doesn't need
  * to concern itself with that
  */
class CompanyIdToIpRangeMapping private (val df: DataFrame)

object CompanyIdToIpRangeMapping {
  val COMPANY_ID_NAME = "company_id"
  val RANGE_START_NAME = "ip_range_start"
  val RANGE_END_NAME = "ip_range_end"

  /**
    * Verifies if the schema of provided DataFrame conforms to the format
    * (company_id: Long, ip_range_start: Array[Byte], ip_range_end: Array[Byte])
    * Range includes ip_range_end
    */
  def apply(inDf: DataFrame): CompanyIdToIpRangeMapping = {
    val startTransformed = TypesUtils.addIpFormattingStep(inDf, RANGE_START_NAME)
    val bothTransformed = TypesUtils.addIpFormattingStep(startTransformed, RANGE_END_NAME)

    new CompanyIdToIpRangeMapping(bothTransformed)
  }
}

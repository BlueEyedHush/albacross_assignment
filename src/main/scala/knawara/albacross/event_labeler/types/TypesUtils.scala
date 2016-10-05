package knawara.albacross.event_labeler.types

import org.apache.spark.sql.DataFrame

object TypesUtils {
  def copyIpColumnAndConvertToString(df: DataFrame, sourceColumnName: String, targetColumnName: String) = {
    import com.google.common.io.BaseEncoding
    /* it's anyval because for some reason SparkSQL uses tinyint instead of binary
     * when it was Array[Byte] an exception was thrown */
    val c: Seq[AnyVal] => String =
      ba =>  BaseEncoding.base32Hex().encode(ba.map(_.asInstanceOf[Byte]).toArray)

    import org.apache.spark.sql.functions.udf
    val converter = udf(c)

    df.withColumn(targetColumnName, converter(df(sourceColumnName)))
  }
}

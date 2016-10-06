package knawara.albacross.event_labeler.types

import java.util

import knawara.albacross.event_labeler.TestUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.scalatest.{FlatSpec, Matchers}

class TypeUtilsTest extends FlatSpec with Matchers {
  import TypesUtils._
  val sqlContext = TestUtils.getSqlContext()

  "convertIpAddress" should "properly format IPv6 address (1)" in {
    convertIpAddress("::1") should be ("0000:0000:0000:0000:0000:0000:0000:0001")
  }

  "convertIpAddress" should "properly format IPv6 address (2)" in {
    convertIpAddress("5::5") should be ("0005:0000:0000:0000:0000:0000:0000:0005")
  }

  "DataFrame processing" should "fail when IPv4 address is inside" in {
    val cn = "column"
    val df = buildDfWithIpColumn(sqlContext, cn)
    val transformedDf = addIpFormattingStep(df, cn)

    assertThrows[IPv4NotSupported]{
      transformedDf.collect()
    }
  }

  private def buildDfWithIpColumn(sqlContext: SQLContext, columnName: String): DataFrame = {
    val rows = new util.ArrayList[Row](1)
    val ipv4row = Tuple1("1.1.1.1")
    rows.add(Row.fromTuple(ipv4row))

    val schema = StructType(Seq(StructField(columnName, StringType, nullable = false)))

    sqlContext.createDataFrame(rows, schema)
  }
}

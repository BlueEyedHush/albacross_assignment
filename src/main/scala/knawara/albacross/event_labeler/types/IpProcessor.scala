package knawara.albacross.event_labeler.types

import java.net.{Inet4Address, Inet6Address, InetAddress}

import org.apache.spark.sql.DataFrame

object IpProcessor {
  def convertIpAddress(ip: String) = {
    InetAddress.getByName(ip) match {
      case ipv4: Inet4Address => throw new IPv4NotSupported
      case ipv6: Inet6Address => ipv6.getHostAddress.split(":").iterator.map(leftPad(_, 4, '0')).mkString(":")
    }
  }

  def addIpFormattingStep(df: DataFrame, columnName: String): DataFrame = {
    import org.apache.spark.sql.functions.udf
    val converter = udf(convertIpAddress _)

    df.withColumn(columnName, converter(df(columnName)))
  }

  private def leftPad(str: String, desiredLength: Int, paddingChar: Char): String = {
    if(str.length >= desiredLength) str
    else {
      val b = new StringBuilder
      for(i <- 1 to (desiredLength - str.length)) b.append(paddingChar)
      b.append(str)
      b.toString
    }
  }
}

sealed class IPv4NotSupported extends RuntimeException

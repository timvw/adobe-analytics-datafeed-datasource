package be.icteam.adobe.analytics.datafeed.util

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.io.Source

case object MobileAttributes {

  def getHeaders(): List[String] = {
    val in = getClass.getResourceAsStream("/mobile_attributes_headers.tsv")
    Source.fromInputStream(in).getLines().flatMap(_.split('\t')).toList
  }

  def getSchema(): StructType = {
    val headerFields = getHeaders().map(x => StructField(x, StringType))
    StructType(headerFields)
  }

}

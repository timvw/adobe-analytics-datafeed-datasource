package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.SparkSession

case object TestUtil {

  def getSparkSession(): SparkSession = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
}

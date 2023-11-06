package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
// needed for 'the read clickstream extension method works' test
import be.icteam.adobe.analytics.datafeed.implicits._
import org.scalatest.funsuite.AnyFunSuite

class DefaultSourceTest extends AnyFunSuite {

  val feedPath = "./src/test/resources/randyzwitch"

  ignore("generic file source options work as expected") {

    val spark = TestUtil.getSparkSession()

    val df = spark.read
      .format("datafeed")
      .option(DatafeedOptions.MODIFIED_AFTER, "2023-11-01T14:56:00")
      .load(feedPath)

    assert(df.count() == 0)

    spark.stop()
  }

  test("spark.read.datafeed extension method works") {
    val spark: SparkSession = TestUtil.getSparkSession()
    assertCompiles("""spark.read.datafeed("./src/test/resources/randyzwitch")""".stripMargin)
    spark.stop()
  }

  test("read datafeed with user provided schema") {
    val spark = TestUtil.getSparkSession()
    val userSchema = StructType(Array(StructField("accept_language", StringType, true)))
    val df = spark.read.format("datafeed").schema(userSchema).load(feedPath)
    assert(df.schema == userSchema)
    assertThrows[AnalysisException](df.select("browser").show(1, false))
    spark.stop()
  }

  test("read datafeed with lookup enrichment") {
    val spark = TestUtil.getSparkSession()

    val df = spark.read
      .format("datafeed")
      .load(feedPath)
      .select(col("os"))
      .orderBy(col("last_hit_time_gmt"))

    val os = df.take(1)(0).getString(0)
    assert(os == "OS X 10.9.5")

    spark.stop()
  }

  test("read datafeed without lookup enrichment") {
    val spark = TestUtil.getSparkSession()

    val df = spark.read
      .format("datafeed")
      .option(DatafeedOptions.ENABLE_LOOKUPS, "false")
      .load(feedPath)
      .select(col("os"))
      .orderBy(col("last_hit_time_gmt"))

    val os = df.take(1)(0).getString(0)
    assert(os == "1550374905")

    spark.stop()
  }
}

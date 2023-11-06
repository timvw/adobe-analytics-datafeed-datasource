package be.icteam.adobe.analytics.datafeed

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, DateTimeUtils}

import java.nio.charset.Charset

trait DataSourceOptions {
  def newOption(name: String): String = name
}

/** *
 * Represents reading options which can be specified for the Datafeed datasource
 * @param parameters
 */
case class DatafeedOptions(parameters: CaseInsensitiveMap[String]) extends Logging with Serializable {

  import implicits._
  import DatafeedOptions._

  /***
   * Defines the maximum number of characters allowed for any given value being written/read. Used to avoid OutOfMemoryErrors (defaults to 4096).
   * To enable auto-expansion of the internal array, set this property to -1
   */
  val maxCharsPerColumn = parameters.getInt(MAX_CHARS_PER_COLUMN, -1)

  /***
   * Defines the file encoding
   */
  def fileEncoding = Charset.forName(parameters.getOrElse(FILE_ENCODING, "ISO-8859-1"))

  val enableLookups = parameters.getBool(ENABLE_LOOKUPS, true)
}

object DatafeedOptions extends DataSourceOptions {

  // source name
  val SOURCE_NAME = newOption(classOf[DefaultSource].getPackage.getName)

  // generic file data source options
  val TIME_ZONE = newOption(DateTimeUtils.TIMEZONE_OPTION)
  val MODIFIED_BEFORE = newOption("modifiedBefore")
  val MODIFIED_AFTER = newOption("modifiedAfter")
  val PATH_GLOB_FILTER = newOption("pathGlobFilter")

  // Datafeed specific options
  val MAX_CHARS_PER_COLUMN = newOption("maxCharsPerColumn")
  val FILE_ENCODING = newOption("fileEncoding")
  val ENABLE_LOOKUPS = newOption("enableLookups")
}

package be.timvw.adobe.analytics.datafeed

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import java.util.Locale

package object implicits {

  /**
   * Extends the DataFrameReader API by adding a Datafeed function
   * Usage:
   * {{{
   * spark.read.datafeed(path)
   * }}}
   */
  implicit class DatafeedDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def datafeed(path: String): DataFrame = {
      reader.format(DatafeedOptions.SOURCE_NAME).load(path)
    }
  }

  /**
   * Extends the DataStreamReader API by adding a Datafeed function
   * Usage:
   * {{{
   * spark.readStream.datafeed(path)
   * }}}
   */
  implicit class DatafeedDataStreamReader(val dataStreamReader: DataStreamReader) extends AnyVal {
    def datafeed(path: String): DataFrame = {
      dataStreamReader.format(DatafeedOptions.SOURCE_NAME).load(path)
    }
  }

  /**
   * Extends the CaseInsensitiveMap[String] interface by exposing utility methods to parse (configuration) values
   * @param parameters
   */
  implicit class CaseInsensitiveMapExt(parameters: CaseInsensitiveMap[String]) {

    object Errors {
      def paramExceedOneCharError(paramName: String): Throwable = {
        new RuntimeException(s"$paramName cannot be more than one character")
      }

      def paramIsNotIntegerError(paramName: String, value: String): Throwable = {
        new RuntimeException(s"$paramName should be an integer. Found ${value}")
      }

      def paramIsNotBooleanValueError(paramName: String): Throwable = {
        new Exception(s"$paramName flag can be true or false")
      }
    }

    def getChar(paramName: String, default: Char): Char = {
      val paramValue = parameters.get(paramName)
      paramValue match {
        case None => default
        case Some(null) => default
        case Some(value) if value.length == 0 => '\u0000'
        case Some(value) if value.length == 1 => value.charAt(0)
        case _ => throw Errors.paramExceedOneCharError(paramName)
      }
    }

    def getInt(paramName: String, default: Int): Int = {
      val paramValue = parameters.get(paramName)
      paramValue match {
        case None => default
        case Some(null) => default
        case Some(value) => try {
          value.toInt
        } catch {
          case e: NumberFormatException =>
            throw Errors.paramIsNotIntegerError(paramName, value)
        }
      }
    }

    def getBool(paramName: String, default: Boolean = false): Boolean = {
      val param = parameters.getOrElse(paramName, default.toString)
      if (param == null) {
        default
      } else if (param.toLowerCase(Locale.ROOT) == "true") {
        true
      } else if (param.toLowerCase(Locale.ROOT) == "false") {
        false
      } else {
        throw Errors.paramIsNotBooleanValueError(paramName)
      }
    }
  }
}

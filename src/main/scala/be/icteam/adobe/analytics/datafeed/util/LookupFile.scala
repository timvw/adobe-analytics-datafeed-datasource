package be.icteam.adobe.analytics.datafeed.util

import be.icteam.adobe.analytics.datafeed.DatafeedOptions
import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.io.File
import java.nio.charset.Charset

case class LookupFile(path: Path, md5: String, size: String) {
  def filesetName = LookupFile.filesetName(this)
}

case object LookupFile {

  def extract(conf: Configuration, path: Path): List[(String, File)] = {
    val fs = FileSystem.get(path.toUri, conf)
    val dataInputStream = fs.open(path)
    try {
      val tarInput = new TarArchiveInputStream(new GzipCompressorInputStream(dataInputStream))
      Iterator
        .continually(tarInput)
        .takeWhile(_.getNextTarEntry != null)
        .map(tarInput => {
          val currentEntry = tarInput.getCurrentEntry
          val lookupFileName = currentEntry.getName
          val lookupFile = File.createTempFile(lookupFileName, "")
          FileUtils.copyToFile(tarInput, lookupFile)
          (lookupFileName, lookupFile)
        })
        .toList
    } finally {
      dataInputStream.close()
      fs.close()
    }
  }

  def getSchemaForDataFiles(lookupFilesByName: Map[String, File], options: DatafeedOptions): StructType = {
    val columnHeadersFile = lookupFilesByName.get(LookupFile.Names.column_headers).get
    val dataFieldNames = LookupFile.ColumnHeaders.getDataColumnNames(columnHeadersFile, options.fileEncoding)
    val dataFields = dataFieldNames.map(x => StructField(x, StringType, true))
    StructType(dataFields)
  }

  def filesetName(lookupFile: LookupFile): String = lookupFile.path.getName.replace("-lookup_data.tar.gz", "")

  case object ColumnHeaders {
    def getDataColumnNames(localFile: File, encoding: Charset): Array[String] = {
      val tsvParserSettings = new TsvParserSettings
      tsvParserSettings.setMaxColumns(10000)
      val tokenizer = new TsvParser(tsvParserSettings)
      val lines = tokenizer.parseAll(localFile, encoding, 1)
      lines.get(0)
    }
  }

  case object Names {
    val browser = "browser.tsv"
    val browser_type = "browser_type.tsv"
    val carrier = "carrier.tsv"
    val color_depth = "color_depth.tsv"
    val column_headers = "column_headers.tsv"
    val connection_type = "connection_type.tsv"
    val country = "country.tsv"
    val event = "event.tsv"
    val javascript_version = "javascript_version.tsv"
    val languages = "languages.tsv"
    val mobile_attributes = "mobile_attributes.tsv"
    val operating_system_type = "operating_system_type.tsv"
    val operating_systems = "operating_systems.tsv"
    val plugins = "plugins.tsv"
    val referrer_type = "referrer_type.tsv"
    val resolution = "resolution.tsv"
    val search_engines = "search_engines.tsv"
  }
}

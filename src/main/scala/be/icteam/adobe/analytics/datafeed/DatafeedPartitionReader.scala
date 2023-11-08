package be.icteam.adobe.analytics.datafeed

import be.icteam.adobe.analytics.datafeed.contributor.ValuesContributor
import be.icteam.adobe.analytics.datafeed.util.LookupFile
import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.CodecStreams
import org.apache.spark.sql.types.StructType

case class DatafeedPartitionReader(conf: Configuration, partition: DatafeedPartition, requestedSchema: StructType, options: DatafeedOptions) extends PartitionReader[InternalRow] {

  val lookupFilesByName = partition.manifestFile.extractLookupFiles(conf)
  val schemaForDatafile = LookupFile.getSchemaForDataFiles(lookupFilesByName, options)

  // https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-spec-chars.html?lang=en
  val dataFileInputStream = CodecStreams.createInputStreamWithCloseResource(conf, partition.dataFile.path)
  val tsvParserSettings = new TsvParserSettings
  tsvParserSettings.setLineJoiningEnabled(true)
  tsvParserSettings.setMaxColumns(schemaForDatafile.length + 1)
  tsvParserSettings.setMaxCharsPerColumn(options.maxCharsPerColumn)
  val tokenizer = new TsvParser(tsvParserSettings)
  val iter = tokenizer.iterate(dataFileInputStream).iterator()

  val valuesContributor = ValuesContributor(options.enableLookups, lookupFilesByName, schemaForDatafile)
  val contributor = valuesContributor.getContributor(List.empty, requestedSchema)

  override def next(): Boolean = {
    iter.hasNext
  }

  override def get(): InternalRow = {
    val row = new GenericInternalRow(requestedSchema.length)
    val columns = iter.next()
    contributor.contributeFunction(row, columns)
    row
  }

  override def close(): Unit = {
    dataFileInputStream.close()
  }
}
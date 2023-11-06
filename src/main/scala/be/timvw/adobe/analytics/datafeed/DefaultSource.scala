package be.timvw.adobe.analytics.datafeed

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

class DefaultSource extends FileDataSourceV2 {
  override def shortName(): String = "datafeed"

  override def fallbackFileFormat: Class[_ <: FileFormat] = ???

  override def getTable(options: CaseInsensitiveStringMap): Table = makeTable(options, None)

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = makeTable(options, Some(schema))

  def makeTable(options: CaseInsensitiveStringMap, maybeSchema: Option[StructType]): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    val DatafeedOptions = new DatafeedOptions(CaseInsensitiveMap(optionsWithoutPaths.asScala.toMap))
    DatafeedTable(tableName, sparkSession, DatafeedOptions, paths, maybeSchema)
  }
}

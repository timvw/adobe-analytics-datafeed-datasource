package be.timvw.adobe.analytics.datafeed

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.PathFilterFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

case class DatafeedTable(name: String,
                         sparkSession: SparkSession,
                         options: DatafeedOptions,
                         paths: Seq[String],
                         maybeUserSpecifiedSchema: Option[StructType]) extends Table with SupportsRead {

  val conf = sparkSession.sparkContext.hadoopConfiguration

  def findManifestFilesInPaths(): List[ManifestFile] = {
    val pathFilters = PathFilterFactory.create(options.parameters)
    paths.flatMap(x => ManifestFile.getManifestFilesInPath(conf, new Path(x), pathFilters)).toList
  }

  lazy val manifestFiles = findManifestFilesInPaths()

  override def schema(): StructType = maybeUserSpecifiedSchema.getOrElse(inferSchema())

  def inferSchema(): StructType = {
    // in case no manifests are found, we blow up...
    val manifestFile = manifestFiles.head
    val lookupFilesByName = manifestFile.extractLookupFiles(conf)
    val schemaForDataFiles = LookupFile.getSchemaForDataFiles(lookupFilesByName, options)

    val valuesContributor = ValuesContributor(options.enableLookups, lookupFilesByName, schemaForDataFiles)
    val fieldsWhichCanBeContributed = valuesContributor.getFieldsWhichCanBeContributed()
    StructType(fieldsWhichCanBeContributed)
  }

  override def capabilities(): util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    DatafeedScanBuilder(sparkSession, this.options, manifestFiles, schema)
  }
}
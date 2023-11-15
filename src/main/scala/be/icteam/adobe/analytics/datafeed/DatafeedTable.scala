package be.icteam.adobe.analytics.datafeed

import be.icteam.adobe.analytics.datafeed.contributor.ValuesContributor
import be.icteam.adobe.analytics.datafeed.util.{LookupFile, ManifestFile}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.PathFilterFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

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

  lazy val inferredSchema: StructType = {
    // in case no manifests are found, we blow up...
    val manifestFile = manifestFiles.head
    val lookupFilesByName = manifestFile.extractLookupFiles(conf)
    val schemaForDataFiles = LookupFile.getSchemaForDataFiles(lookupFilesByName, options)

    val valuesContributor = ValuesContributor(manifestFile, options, lookupFilesByName, schemaForDataFiles)
    val fieldsWhichCanBeContributed = valuesContributor.getFieldsWhichCanBeContributed()
    StructType(fieldsWhichCanBeContributed)
  }

  def inferSchema(): StructType = inferredSchema

  override def capabilities(): java.util.Set[TableCapability] = Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    DatafeedScanBuilder(sparkSession, this.options, manifestFiles, schema)
  }
}
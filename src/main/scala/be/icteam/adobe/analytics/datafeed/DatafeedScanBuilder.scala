package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.types.StructType

case class DatafeedScanBuilder(sparkSession: SparkSession,
                               options: DatafeedOptions,
                               manifestFiles: List[ManifestFile],
                               schema: StructType) extends ScanBuilder with SupportsPushDownRequiredColumns {

  var maybeRequestedSchema: Option[StructType] = None

  override def build(): Scan = {
    val requestedSchema = maybeRequestedSchema.getOrElse(schema)
    DatafeedScan(sparkSession, manifestFiles, requestedSchema, options)
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    maybeRequestedSchema = Some(requiredSchema)
  }
}

package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class DatafeedScan(sparkSession: SparkSession, manifestFiles: Seq[ManifestFile], requestedSchema: StructType, options: DatafeedOptions)
  extends Scan with Batch {

  override def readSchema(): StructType = requestedSchema

  override def planInputPartitions(): Array[InputPartition] = {
    manifestFiles
      .flatMap(manifestFile => manifestFile.dataFiles.map(dataFile => DatafeedPartition(dataFile, manifestFile, options)))
      .toArray
  }

  override def toBatch: Batch = this

  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = Map.empty[String, String]
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    DatafeedPartitionReaderFactory(broadcastedConf, requestedSchema, options)
  }
}

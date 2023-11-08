package be.icteam.adobe.analytics.datafeed

import be.icteam.adobe.analytics.datafeed.util.{DataFile, ManifestFile}
import org.apache.spark.sql.connector.read.InputPartition

case class DatafeedPartition(dataFile: DataFile, manifestFile: ManifestFile, options: DatafeedOptions) extends InputPartition

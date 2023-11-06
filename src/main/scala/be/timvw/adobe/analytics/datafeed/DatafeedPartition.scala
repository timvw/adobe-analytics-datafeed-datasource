package be.timvw.adobe.analytics.datafeed

import org.apache.spark.sql.connector.read.InputPartition

case class DatafeedPartition(dataFile: DataFile, manifestFile: ManifestFile, options: DatafeedOptions) extends InputPartition

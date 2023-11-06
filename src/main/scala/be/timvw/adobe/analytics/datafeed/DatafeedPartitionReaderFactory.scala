package be.timvw.adobe.analytics.datafeed

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

case class DatafeedPartitionReaderFactory(broadcastedConf: Broadcast[SerializableConfiguration], requestedSchema: StructType, options: DatafeedOptions) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val conf = broadcastedConf.value.value
    DatafeedPartitionReader(conf, partition.asInstanceOf[DatafeedPartition], requestedSchema, options)
  }
}

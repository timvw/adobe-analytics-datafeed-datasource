package be.icteam.adobe.analytics.datafeed.contributor
import be.icteam.adobe.analytics.datafeed.DatafeedOptions
import be.icteam.adobe.analytics.datafeed.util.ManifestFile
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

case class MetadataValuesContributor(manifestFile: ManifestFile, datafeedOptions: DatafeedOptions) extends ValuesContributor {

  private def prefixFieldName(fieldName: String): String = s"${datafeedOptions.metadataPrefix}$fieldName"

  private val lastModifiedColumnName = prefixFieldName("last_modified")
  private val filesetColumnName = prefixFieldName("fileset")

  case class MetadataField(name: String, dataType: DataType, value: Any)

  private val metadataFields = List(
    MetadataField(lastModifiedColumnName, TimestampType, manifestFile.modificationTime*1000),
    MetadataField(filesetColumnName, StringType, UTF8String.fromString(manifestFile.filesetName))
  )

  override def getFieldsWhichCanBeContributed(): List[StructField] = metadataFields.map(metadataFieldToStructField)

  private def metadataFieldToStructField(x: MetadataField): StructField = StructField(x.name, x.dataType, false)

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {
    def metadataFieldIsRequested(f: MetadataField): Boolean = requestedSchema.fieldNames.contains(f.name)
    val contributedMetadataFields = metadataFields.filter(metadataFieldIsRequested)
    val contributedFields = contributedMetadataFields.map(metadataFieldToStructField)

    val contributeFunctions = contributedMetadataFields.map(fieldToContribute => {
      val requestedFieldIndex = requestedSchema.fieldIndex(fieldToContribute.name)
      (row: GenericInternalRow, _: Array[String]) => {
        val value = fieldToContribute.value
        row.update(requestedFieldIndex, value)
      }
    })

    val contributeFunction = (row: GenericInternalRow, parsedValues: Array[String]) => {
      contributeFunctions.foreach(x => x(row, parsedValues))
    }

    Contributor(contributedFields, contributeFunction)
  }
}

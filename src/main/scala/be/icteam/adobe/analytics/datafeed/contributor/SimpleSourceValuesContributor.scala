package be.icteam.adobe.analytics.datafeed.contributor

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

case class SimpleSourceValuesContributor(sourceSchema: StructType) extends ValuesContributor {

  override def getFieldsWhichCanBeContributed(): List[StructField] = sourceSchema.fields.toList

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {

    def requestedFieldExistsInSource(requestedField: StructField): Boolean = sourceSchema.fieldNames.contains(requestedField.name)

    def fieldIsNotAlreadyContributed(requestedField: StructField): Boolean = !alreadyContributedFields.map(_.name).contains(requestedField.name)

    val contributedFields = requestedSchema.fields
      .filter(requestedFieldExistsInSource)
      .filter(fieldIsNotAlreadyContributed)
      .toList

    val contributeFunctions = contributedFields.map(fieldToContribute => {
      val physicalFieldIndex = sourceSchema.fieldIndex(fieldToContribute.name)
      val requestedFieldIndex = requestedSchema.fieldIndex(fieldToContribute.name)
      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else UTF8String.fromString(parsedValue)
        row.update(requestedFieldIndex, value)
      }
    })

    val contributeFunction = (row: GenericInternalRow, parsedValues: Array[String]) => {
      contributeFunctions.foreach(x => x(row, parsedValues))
    }

    Contributor(contributedFields, contributeFunction)
  }
}

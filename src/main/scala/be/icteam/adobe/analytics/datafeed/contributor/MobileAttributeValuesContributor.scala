package be.icteam.adobe.analytics.datafeed.contributor

import be.icteam.adobe.analytics.datafeed.util.{LookupDatabase, LookupFile, MobileAttributes}
import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File
import java.nio.ByteBuffer


/** *
 * Contributes MobileAttributes
 *
 * @see https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/dynamic-lookups.html?lang=en
 * @param lookupFilesByName
 * @param sourceSchema
 */
case class MobileAttributeValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

  override def getFieldsWhichCanBeContributed(): List[StructField] = rulesWhichCanContribute.map(_.resultSchemaField)

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {
    val contributingLookupRules = getContributingRules(requestedSchema)
    buildLookupDatabases(contributingLookupRules)
    val contributedFields = contributingLookupRules.map(_.resultSchemaField)

    val contributeFunctions = contributingLookupRules.map(simpleLookupRule => {
      val physicalFieldIndex = sourceSchema.fieldIndex(simpleLookupRule.phyiscalColumnName)
      val requestedFieldIndex = requestedSchema.fieldIndex(simpleLookupRule.resultSchemaField.name)
      val lookupDatabase = lookupDatabasesByName(simpleLookupRule.lookupfileName)
      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else {
          lookupDatabase.get(parsedValue.getBytes)
        }
        row.update(requestedFieldIndex, value)
      }
    })

    val contributeFunction = (row: GenericInternalRow, parsedValues: Array[String]) => {
      contributeFunctions.foreach(x => x(row, parsedValues))
    }

    Contributor(contributedFields, contributeFunction)
  }

  /** *
   * Represents a rule which specified how the resulting schema should be enriched with a value being looked up in the lookupfile
   *
   * @param lookupfileName     the name of the file in which the value will searched for the given key
   * @param phyiscalColumnName the name of the column in the hitdata file which will be used as key for lookup
   * @param resultSchemaField  the field in which the result of the lookup will be stored
   */
  private case class SimpleLookupRule(lookupfileName: String, phyiscalColumnName: String, resultSchemaField: StructField)

  private lazy val mobileAttributesSchema = {
    val fullSchema = MobileAttributes.getSchema()
    StructType(fullSchema.fields.filter(x => x.name != "mobile_id"))
  }

  private val simpleLookupRules = List(
    SimpleLookupRule(LookupFile.Names.mobile_attributes, "mobile_id", StructField("mobile_attributes", mobileAttributesSchema, true)),
  )

  private val rulesWhichCanContribute = {
    def fileExistsForLookupRule(simpleLookupRule: SimpleLookupRule): Boolean = lookupFilesByName.contains(simpleLookupRule.lookupfileName)

    def sourceFieldExistsForLookupRule(simpleLookupRule: SimpleLookupRule): Boolean = sourceSchema.fieldNames.contains(simpleLookupRule.phyiscalColumnName)

    simpleLookupRules
      .filter(fileExistsForLookupRule)
      .filter(sourceFieldExistsForLookupRule)
  }

  private def getContributingRules(requestedSchema: StructType) = {
    def lookupFieldIsRequested(simpleLookupRule: SimpleLookupRule): Boolean = requestedSchema.fieldNames.contains(simpleLookupRule.resultSchemaField.name)

    rulesWhichCanContribute.filter(lookupFieldIsRequested)
  }

  var lookupDatabasesByName: Map[String, LookupDatabase[GenericInternalRow]] = _

  override def close(): Unit = {
    Option(lookupDatabasesByName).foreach(x => x.foreach(_._2.close()))
  }

  private def buildLookupValue(values: Array[String]): GenericInternalRow = {
    val row = new GenericInternalRow(mobileAttributesSchema.length)
    values.zipWithIndex.foreach { case (x, idx) => row.update(idx, UTF8String.fromString(x)) }
    row
  }

  private val kryoSerializer = new KryoSerializer(new SparkConf()).newInstance()
  private def serialize(x: GenericInternalRow): Array[Byte] = kryoSerializer.serialize(x).array()
  private def deserialize(bytes: Array[Byte]): GenericInternalRow = {
    if(bytes == null) null else kryoSerializer.deserialize(ByteBuffer.wrap(bytes))
  }

  private def buildLookupFileDatabase(lookupFileName: String) = {
    val lookupFile = lookupFilesByName(lookupFileName)
    LookupDatabase(lookupFile, buildLookupValue, serialize, deserialize)
  }

  private def buildLookupDatabases(contributingLookupRules: Seq[SimpleLookupRule]): Unit = {
    val contributingLookupFiles = contributingLookupRules
      .map(_.lookupfileName)
      .toSet

    lookupDatabasesByName = contributingLookupFiles
      .map(x => (x, buildLookupFileDatabase(x)))
      .toMap
  }
}

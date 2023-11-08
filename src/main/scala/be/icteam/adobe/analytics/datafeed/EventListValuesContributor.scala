package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File

case class EventListValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

  private case class ListLookupRule(lookupfileName: String, phyiscalColumnName: String, resultSchemaField: StructField)

  private val listLookupRules = List(
    ListLookupRule(LookupFile.Names.event, "event_list", StructField("event_list", ArrayType(StringType))),
    ListLookupRule(LookupFile.Names.event, "post_event_list", StructField("post_event_list", ArrayType(StringType))))

  override def getFieldsWhichCanBeContributed(): List[StructField] = rulesWhichCanContribute.map(_.resultSchemaField)

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {

    val contributingLookupRules = getContributingRules(requestedSchema)
    buildLookupDatabases(contributingLookupRules)
    val contributedFields = contributingLookupRules.map(_.resultSchemaField)

    val contributeFunctions = contributingLookupRules.map(simpleLookupRule => {
      val physicalFieldIndex = sourceSchema.fieldIndex(simpleLookupRule.phyiscalColumnName)
      val requestedFieldIndex = requestedSchema.fieldIndex(simpleLookupRule.resultSchemaField.name)
      //val lookupDatabase = lookupDatabasesByName(simpleLookupRule.lookupfileName)
      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else {
          // lookup the thingie...
          null
        }
        row.update(requestedFieldIndex, value)
      }
    })

    val contributeFunction = (row: GenericInternalRow, parsedValues: Array[String]) => {
      contributeFunctions.foreach(x => x(row, parsedValues))
    }

    Contributor(contributedFields, contributeFunction)
  }

  private val rulesWhichCanContribute = {
    def fileExistsForLookupRule(listLookupRule: ListLookupRule): Boolean = lookupFilesByName.contains(listLookupRule.lookupfileName)

    def sourceFieldExistsForLookupRule(listLookupRule: ListLookupRule): Boolean = sourceSchema.fieldNames.contains(listLookupRule.phyiscalColumnName)

    listLookupRules
      .filter(fileExistsForLookupRule)
      .filter(sourceFieldExistsForLookupRule)
  }

  private def getContributingRules(requestedSchema: StructType) = {
    def lookupFieldIsRequested(listLookupRule: ListLookupRule): Boolean = requestedSchema.fieldNames.contains(listLookupRule.resultSchemaField.name)

    rulesWhichCanContribute.filter(lookupFieldIsRequested)
  }

  var lookupDatabasesByName: Map[String, LookupDatabase] = _

  override def close(): Unit = {
    Option(lookupDatabasesByName).foreach(x => x.foreach(_._2.close()))
  }

  private def buildLookupFileDatabase(lookupFileName: String) = {
    val lookupFile = lookupFilesByName(lookupFileName)
    LookupDatabase(lookupFile)
  }

  private def buildLookupDatabases(contributingLookupRules: Seq[ListLookupRule]): Unit = {
    val contributingLookupFiles = contributingLookupRules
      .map(_.lookupfileName)
      .toSet

    lookupDatabasesByName = contributingLookupFiles
      .map(x => (x, buildLookupFileDatabase(x)))
      .toMap
  }
}

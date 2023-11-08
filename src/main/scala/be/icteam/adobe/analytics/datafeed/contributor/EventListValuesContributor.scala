package be.icteam.adobe.analytics.datafeed.contributor

import be.icteam.adobe.analytics.datafeed.util.{LookupDatabase, LookupFile}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File
import java.util.regex.Pattern

case class EventListValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

  private case class ListLookupRule(lookupfileName: String, phyiscalColumnName: String, resultSchemaField: StructField)

  private val eventSchema = StructType(Array(
    StructField("event", StringType),
    StructField("value", StringType)))

  private val listLookupRules = List(
    ListLookupRule(LookupFile.Names.event, "event_list", StructField("event_list", ArrayType(eventSchema))),
    ListLookupRule(LookupFile.Names.event, "post_event_list", StructField("post_event_list", ArrayType(eventSchema))))

  override def getFieldsWhichCanBeContributed(): List[StructField] = rulesWhichCanContribute.map(_.resultSchemaField)

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {

    val contributingLookupRules = getContributingRules(requestedSchema)
    buildLookupDatabases(contributingLookupRules)
    val contributedFields = contributingLookupRules.map(_.resultSchemaField)

    val contributeFunctions = contributingLookupRules.map(simpleLookupRule => {
      val physicalFieldIndex = sourceSchema.fieldIndex(simpleLookupRule.phyiscalColumnName)
      val requestedFieldIndex = requestedSchema.fieldIndex(simpleLookupRule.resultSchemaField.name)
      val lookupDatabase = lookupDatabasesByName(simpleLookupRule.lookupfileName)

      val itemSeparatorPattern = Pattern.compile("(?<!\\^),")
      val assignmentPattern = Pattern.compile("(?<!\\^)=")

      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else {
          // 20117=3.06,20118,100,102
          val items = itemSeparatorPattern.split(parsedValue)
          val itemList = items.map(x => {
            val parts = assignmentPattern.split(x,2)
            val eventRow = new GenericInternalRow(2)
            val foundEvent = lookupDatabase.get(parts(0).getBytes)
            eventRow.update(0, UTF8String.fromBytes(foundEvent))
            if(parts.length > 1) {
              eventRow.update(1, UTF8String.fromString(parts(1)))
            }
            eventRow
          })
          ArrayData.toArrayData(itemList)
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

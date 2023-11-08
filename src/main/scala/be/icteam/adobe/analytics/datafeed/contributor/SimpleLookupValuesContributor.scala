package be.icteam.adobe.analytics.datafeed.contributor

import be.icteam.adobe.analytics.datafeed.util.{LookupDatabase, LookupFile}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File



  case class SimpleLookupValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

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
          val foundValue = lookupDatabase.get(parsedValue.getBytes)
          UTF8String.fromBytes(foundValue)
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

  private val simpleLookupRules = List(
    SimpleLookupRule(LookupFile.Names.browser, "browser", StructField("browser", StringType, true)),
    SimpleLookupRule(LookupFile.Names.browser_type, "browser", StructField("browser_type", StringType, true)),
    SimpleLookupRule(LookupFile.Names.carrier, "carrier", StructField("carrier", StringType, true)),
    SimpleLookupRule(LookupFile.Names.color_depth, "color", StructField("color", StringType, true)),
    SimpleLookupRule(LookupFile.Names.connection_type, "connection_type", StructField("connection_type", StringType, true)),
    SimpleLookupRule(LookupFile.Names.country, "country", StructField("country", StringType, true)),
    SimpleLookupRule(LookupFile.Names.javascript_version, "javascript", StructField("javascript", StringType, true)),
    SimpleLookupRule(LookupFile.Names.languages, "language", StructField("language", StringType, true)),
    SimpleLookupRule(LookupFile.Names.operating_systems, "os", StructField("os", StringType, true)),
    SimpleLookupRule(LookupFile.Names.operating_system_type, "os", StructField("os_type", StringType, true)),
    SimpleLookupRule(LookupFile.Names.plugins, "plugin", StructField("plugin", StringType, true)),
    SimpleLookupRule(LookupFile.Names.resolution, "resolution", StructField("resolution", StringType, true)),
    SimpleLookupRule(LookupFile.Names.referrer_type, "ref_type", StructField("ref_type", StringType, true)),
    SimpleLookupRule(LookupFile.Names.search_engines, "search_engine", StructField("search_engine", StringType, true)),
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

  var lookupDatabasesByName: Map[String, LookupDatabase] = _

  override def close(): Unit = {
    Option(lookupDatabasesByName).foreach(x => x.foreach(_._2.close()))
  }

  private def buildLookupFileDatabase(lookupFileName: String) = {
    val lookupFile = lookupFilesByName(lookupFileName)
    LookupDatabase(lookupFile)
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

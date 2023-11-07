package be.icteam.adobe.analytics.datafeed

import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.rocksdb.{Options, RocksDB}

import java.io.{ByteArrayInputStream, File, FileInputStream}
import java.nio.file.Files

import scala.collection.JavaConverters._

case class ListLookupValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

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
      val lookupDatabase = lookupDatabasesByName(simpleLookupRule.lookupfileName)
      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else {
          val items = parsedValue.split(",")
          val lookupedValuesAndProperties = items.map(x => {
            if(x.contains("=")) {
              UTF8String.fromString(x)
            } else {
              val foundValue = lookupDatabase.get(x.getBytes)
              if (foundValue == null) null else UTF8String.fromBytes(foundValue)
            }
          })
          ArrayData.toArrayData(lookupedValuesAndProperties)
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

  var lookupDatabasesByName: Map[String, RocksDB] = _

  override def close(): Unit = {
    Option(lookupDatabasesByName).foreach(x => x.foreach(_._2.close()))
  }

  private def buildLookupFileDatabase(lookupFileName: String) = {
    RocksDB.loadLibrary()

    val options = new Options()
      .setCreateIfMissing(true)
      .setUseDirectReads(true)

    val lookupFileDbDir = Files.createTempDirectory(s"lookups-${lookupFileName}")
    val lookupFileDb = RocksDB.open(options, lookupFileDbDir.toString)

    import scala.collection.JavaConverters._
    val lookupFile = lookupFilesByName(lookupFileName)
    val lookupStream = new FileInputStream(lookupFile)
    val tsvParserSettings = new TsvParserSettings
    tsvParserSettings.setMaxColumns(10)
    val tokenizer = new TsvParser(tsvParserSettings)
    tokenizer.iterate(lookupStream).iterator().asScala.foreach(x => {
      lookupFileDb.put(x(0).getBytes, x(1).getBytes)
    })
    lookupStream.close()
    lookupFileDb
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

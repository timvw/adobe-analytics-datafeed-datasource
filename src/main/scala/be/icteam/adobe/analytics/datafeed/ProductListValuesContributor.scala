package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String

import java.io.File

case class ProductListValuesContributor(lookupFilesByName: Map[String, File], sourceSchema: StructType) extends ValuesContributor with AutoCloseable {

  private case class ListLookupRule(lookupfileName: String, phyiscalColumnName: String, resultSchemaField: StructField)

  private val keyValueSchema = StructType(Array(
    StructField("key", StringType),
    StructField("value", StringType)
  ))

  private val productSchema = StructType(Array(
    StructField("category", StringType),
    StructField("name", StringType),
    StructField("quantity", StringType),
    StructField("price", StringType),
    StructField("events", ArrayType(keyValueSchema)),
    StructField("evars", ArrayType(keyValueSchema)),
  ))

  private val listLookupRules = List(
    ListLookupRule(LookupFile.Names.event, "product_list", StructField("product_list", ArrayType(productSchema))),
    ListLookupRule(LookupFile.Names.event, "post_product_list", StructField("post_product_list", ArrayType(productSchema))))

  override def getFieldsWhichCanBeContributed(): List[StructField] = rulesWhichCanContribute.map(_.resultSchemaField)

  private def buildKeyValueArray(items: List[(String, String)]): ArrayData = {
    val arrayItems = items.map(x => {
      val row = new GenericInternalRow(2)
      row.update(0, UTF8String.fromString(x._1))
      row.update(1, UTF8String.fromString(x._2))
      row
    })
    ArrayData.toArrayData(arrayItems)
  }

  private def buildProductRow(product: Product): GenericInternalRow = {
    val productRow = new GenericInternalRow(6)
    productRow.update(0, product.category.map(UTF8String.fromString).orNull)
    productRow.update(1, UTF8String.fromString(product.name))
    productRow.update(2, product.quantity.map(UTF8String.fromString).orNull)
    productRow.update(3, product.price.map(UTF8String.fromString).orNull)
    productRow.update(4, product.events.map(buildKeyValueArray).orNull)
    productRow.update(5, product.evars.map(buildKeyValueArray).orNull)
    productRow
  }

  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {

    val contributingLookupRules = getContributingRules(requestedSchema)
    buildLookupDatabases(contributingLookupRules)
    val contributedFields = contributingLookupRules.map(_.resultSchemaField)

    val contributeFunctions = contributingLookupRules.map(simpleLookupRule => {
      val physicalFieldIndex = sourceSchema.fieldIndex(simpleLookupRule.phyiscalColumnName)
      val requestedFieldIndex = requestedSchema.fieldIndex(simpleLookupRule.resultSchemaField.name)

      (row: GenericInternalRow, columns: Array[String]) => {
        val parsedValue = columns(physicalFieldIndex)
        val value = if (parsedValue == null) null else {
          val products = Product.Parser.parseProducts(parsedValue)
          val productItems = products.map(buildProductRow)
          ArrayData.toArrayData(productItems)
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

  private var lookupDatabasesByName: Map[String, LookupDatabase] = _

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

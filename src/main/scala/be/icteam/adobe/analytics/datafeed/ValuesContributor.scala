package be.icteam.adobe.analytics.datafeed

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{StructField, StructType}

import java.io.File

/**
 * Contributes values to a row of Datafeed data
 */
trait ValuesContributor {
  /**
   * Gets the fields which can be contributed (regardless of what was requested)
   * @return
   */
  def getFieldsWhichCanBeContributed(): List[StructField]

  /***
   * Gets the fields which are actually contributed (respecting what was requested and already contributed (by other contributors)
   * AND a function which contributes the values
   * @param alreadyContributedFields
   * @param requestedSchema
   * @return
   */
  def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor
}

case class Contributor(contributedFields: List[StructField], contributeFunction: (GenericInternalRow, Array[String]) => Unit)

case class CompositeValuesContributor(valuesContributors: List[ValuesContributor]) extends ValuesContributor {
  override def getFieldsWhichCanBeContributed(): List[StructField] = valuesContributors.foldLeft(Set.empty[StructField])((acc, x) => acc ++ x.getFieldsWhichCanBeContributed()).toList
  override def getContributor(alreadyContributedFields: List[StructField], requestedSchema: StructType): Contributor = {
    // update thingie..
    val compositeValuesContributorParts = valuesContributors.foldLeft((alreadyContributedFields, List.empty[(GenericInternalRow, Array[String]) => Unit]))((acc, valuesContributor) => {
      val contributor = valuesContributor.getContributor(acc._1, requestedSchema)
      val contributedValues = alreadyContributedFields ++ contributor.contributedFields
      val contributorActions = acc._2 ++ List(contributor.contributeFunction)
      (contributedValues, contributorActions)
    })
    val compositeContributedFields = compositeValuesContributorParts._1
    val compositeContributeAction = (row: GenericInternalRow, values: Array[String]) => {
      compositeValuesContributorParts._2.foreach { x => x(row, values) }
    }
    Contributor(compositeContributedFields, compositeContributeAction)
  }
}

object ValuesContributor {
  def apply(enableLookups: Boolean, lookupFilesByName: Map[String, File], sourceSchema: StructType): ValuesContributor = {
    val contributors = if(enableLookups) {
      List(SimpleLookupValuesContributor(lookupFilesByName, sourceSchema), SimpleSourceValuesContributor(sourceSchema))
    } else {
      List(SimpleSourceValuesContributor(sourceSchema))
    }
    CompositeValuesContributor(contributors)
  }
}
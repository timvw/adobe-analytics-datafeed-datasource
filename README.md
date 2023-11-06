[![Maven Central](https://maven-badges.herokuapp.com/maven-central/be.timvw/adobe-analytics-datafeed-datasource_2.12/badge.svg)](https://central.sonatype.com/artifact/be.timvw/adobe-analytics-datafeed-datasource_2.12)

# Datasource for Adobe Analytics Data Feed

Adobe Analytics [Data feeds](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-contents.html?lang=en) are a means to get raw data out of Adobe Analytics.

This project implements an [Apache Spark data source](https://spark.apache.org/docs/latest/sql-data-sources.html) leveraging [uniVocity TSV Parser](https://github.com/uniVocity/univocity-parsers/tree/master) and does not suffer from the flaws found in many online examples which treat the [(hit)data files](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-contents.html?lang=en#hit-data-files) as CSV.
Concretly, escaped values are not handled correctly by a CSV parser due to inherent [differences between CSV and TSV](https://github.com/eBay/tsv-utils/blob/master/docs/comparing-tsv-and-csv.md).

## Usage

Make sure the package is in the classpath, eg: by using the --packages option:

```bash
spark-shell --packages "be.timvw:adobe-analytics-datafeed-datasource_2.12:0.0.1"
```

And you can read the feed as following:

```scala
val df = spark.read
  .format("be.timvw.adobe.analytics.datafeed")
  .load("./src/test/resources/randyzwitch")
```

## Features

* Correct handling of records which contain [special characters](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-spec-chars.html?lang=en)
* Capability to translate lookup columns with their actual value as specified in the [Lookup files](https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-contents.html?lang=en#lookup-files)

## Options

* FileEncoding (default: ISO-8859-1)
* MaxCharsPerColumn (default: -1)
* EnableLookups (default: true)

We also support the Generic file source options:
* [Path Glob Filter](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html#path-glob-filter) for manifest files (so should end with *.txt)
* [Modification Time Path Filters](https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html#modification-time-path-filters) for manifest files

```scala
val df = spark.read
  .format("be.timvw.adobe.analytics.datafeed")
  .option(ClickstreamOptions.MODIFIED_AFTER, "2023-11-01T00:00:00")
  .load("./src/test/resources/randyzwitch")
```
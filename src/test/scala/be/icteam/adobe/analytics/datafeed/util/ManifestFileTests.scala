package be.icteam.adobe.analytics.datafeed.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files
import java.time.Instant

class ManifestFileTests extends AnyFunSuite {

  test("write a manifest file") {

    val conf = new Configuration()

    val manifestFile = ManifestFile(
      lookupFiles = List(LookupFile(new Path("zwitchdev_2015-07-13-lookup_data.tar.gz"), "", "")),
      dataFiles = List(DataFile(new Path("01-zwitchdev_2015-07-13.tsv.gz"), "", "")),
      modificationTime = Instant.now.toEpochMilli)

    val tempDirectory = Files.createTempDirectory("test")
    val roundtripManifestFilePath = ManifestFile.write(conf, manifestFile, new Path(tempDirectory.toString))

    val fs = FileSystem.get(roundtripManifestFilePath.toUri, new Configuration())
    val roundtripManifestFileStatus = fs.getFileStatus(roundtripManifestFilePath)

    val roundtripManifestFile = ManifestFile.parse(conf, roundtripManifestFileStatus)
    assert(roundtripManifestFile.lookupFiles.size == 1)
    assert(roundtripManifestFile.lookupFiles.head.path.toString.endsWith("zwitchdev_2015-07-13-lookup_data.tar.gz"))
    assert(roundtripManifestFile.dataFiles.size == 1)
    assert(roundtripManifestFile.dataFiles.head.path.toString.endsWith("01-zwitchdev_2015-07-13.tsv.gz"))
  }

  test("generate a list of manifest files from hitdata") {
    val conf = new Configuration()
    val hitdataDirectory = new Path("./src/test/resources/randyzwitch")
    val lookupDataDirectory = new Path("./src/test/resources/randyzwitch")
    val manifests = ManifestFile.generateManifestsFromHitdata(conf, hitdataDirectory, lookupDataDirectory)
    assert(manifests.size == 1)
    assert(manifests.head.lookupFiles.size == 1)
    assert(manifests.head.lookupFiles.head.path.toString.endsWith("zwitchdev_2015-07-13-lookup_data.tar.gz"))
    assert(manifests.head.dataFiles.size == 1)
    assert(manifests.head.dataFiles.head.path.toString.endsWith("01-zwitchdev_2015-07-13.tsv.gz"))
  }
}

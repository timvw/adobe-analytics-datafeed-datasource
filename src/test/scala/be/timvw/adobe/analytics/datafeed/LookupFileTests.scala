package be.timvw.adobe.analytics.datafeed

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class LookupFileTests extends AnyFunSuite {

  val lookupFile = "./src/test/resources/randyzwitch/zwitchdev_2015-07-13-lookup_data.tar.gz"

  test("extract lookup files") {
    assert(new File(lookupFile).exists())
    val extractedFiles = LookupFile.extract(new Configuration(), new Path(lookupFile))
    assert(extractedFiles.size > 0)
  }

  test("infer filesetName") {
    val sut = LookupFile(new Path(lookupFile), "", "")
    assert(sut.filesetName == "zwitchdev_2015-07-13")
  }
}

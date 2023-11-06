package be.icteam.adobe.analytics.datafeed

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class DataFileTests extends AnyFunSuite {

  val dataFile = "./src/test/resources/randyzwitch/01-zwitchdev_2015-07-13.tsv.gz"

  test("infer filesetName") {
    val sut = DataFile(new Path(dataFile), "", "")
    assert(sut.filesetName == "zwitchdev_2015-07-13")
  }

}

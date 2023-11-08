package be.icteam.adobe.analytics.datafeed.util

import org.scalatest.funsuite.AnyFunSuite

class MobileAttributesTests extends  AnyFunSuite {

  test("can read the headers") {
    val headers = MobileAttributes.getHeaders()
    assert(headers.length > 0)
  }

  test("can get schema") {
    val schema = MobileAttributes.getSchema()
    assert(schema.fieldNames.contains("Manufacturer"))
    assert(schema.fieldNames.contains("Device"))
  }
}

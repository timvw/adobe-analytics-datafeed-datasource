package be.icteam.adobe.analytics.datafeed

import org.scalatest.funsuite.AnyFunSuite

class ProductParserTests extends AnyFunSuite {

  // mobile attributes: https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/dynamic-lookups.html?lang=en

  /* examples from: https://experienceleague.adobe.com/docs/analytics/implementation/vars/page-vars/products.html?lang=en */
  test("parse product") {

    import Product.Parser.parseProducts

    // Include only product and category. Common on individual product pages// Include only product and category. Common on individual product pages
    assert(parseProducts("Example category;Example product") == List(
      Product("Example product", category = Some("Example category"))))

    // Include only product name
    assert(parseProducts(";Example product") == List(
      Product("Example product")))

    // One product has a category, the other does not. Note the comma and adjacent semicolon to omit category
    assert(parseProducts("Example category;Example product 1,;Example product 2") == List(
      Product("Example product 1", category = Some("Example category")),
      Product("Example product 2")))

    // A visitor purchases a single product; record quantity and price
    assert(parseProducts(";Example product;1;6.99") == List(
      Product("Example product", quantity = Some("1"), price = Some("6.99"))
    ))

    // A visitor purchases multiple products with different quantities
    assert(parseProducts(";Example product 1;9;26.91,Example category;Example product 2;4;9.96") == List(
      Product("Example product 1", quantity = Some("9"), price = Some("26.91")),
      Product("Example product 2", category = Some("Example category"), quantity = Some("4"), price = Some("9.96"))
    ))

    // Attribute currency event1 only to product 2 and not product 1
    assert(parseProducts(";Example product 1;1;1.99,Example category 2;Example product 2;1;2.69;event1=1.29") == List(
      Product("Example product 1", quantity = Some("1"), price = Some("1.99")),
      Product("Example product 2", category = Some("Example category 2"), quantity = Some("1"), price = Some("2.69"), events = Some(List(("event1", "1.29"))))
    ))

    // Use multiple numeric events in the product string
    assert(parseProducts(";Example product;1;4.20;event1=2.3|event2=5") == List(
      Product("Example product", quantity = Some("1"), price = Some("4.20"), events = Some(List(("event1", "2.3"), ("event2", "5"))))
    ))

    // Use merchandising eVars without any events. Note the adjacent semicolons to skip events
    assert(parseProducts(";Example product;1;6.69;;eVar1=Merchandising value") == List(
      Product("Example product", quantity = Some("1"), price = Some("6.69"), evars = Some(List(("eVar1", "Merchandising value"))))
    ))

    // Use merchandising eVars without category, quantity, price, or events
    assert(parseProducts(";Example product;;;;eVar1=Merchandising value") == List(
      Product("Example product", evars = Some(List(("eVar1", "Merchandising value"))))
    ))

    // With escaped characters
    assert(parseProducts(";Example p^,roduct,;Example p^;roduct2,;Example p^^roduct3;;;;eVar1=M^=e^;rchandising value") == List(
      Product("Example p,roduct"),
      Product("Example p;roduct2"),
      Product("Example p^roduct3", evars = Some(List(("eVar1", "M=e;rchandising value"))))))
  }
}

package be.icteam.adobe.analytics.datafeed

import java.util.regex.Pattern

/***
 *
 * @param name (required): The name of the product. The maximum length for this field is 100 bytes.
 * @param category (optional): The product category. The maximum length for this field is 100 bytes.
 * @param quantity (optional): How many of this product is in the cart. This field only applies to hits with the purchase event.
 * @param price (optional): The total price of the product as a decimal. If quantity is more than one, set price to the total and not the individual product price. Align the currency of this value to match the currencyCode variable. Do not include the currency symbol in this field. This field only applies to hits with the purchase event.
 * @param events (optional): Events tied to the product. Delimit multiple events with a pipe (|). See events for more information.
 * @param evars (optional): Merchandising eVars tied to the product. Delimit multiple merchandising eVars with a pipe (|). See merchandising eVars for more information.
 * @see https://experienceleague.adobe.com/docs/analytics/implementation/vars/page-vars/products.html?lang=en
 * @see https://experienceleague.adobe.com/docs/analytics/export/analytics-data-feed/data-feed-contents/datafeeds-spec-chars.html?lang=en
 */
case class Product(
                    name: String,
                    category: Option[String] = None,
                    quantity: Option[String] = None,
                    price: Option[String] = None,
                    events: Option[List[(String, String)]] = None,
                    evars: Option[List[(String, String)]] = None)

case object Product {

  case object Parser {

    /*

    ,	Comma. Represents the end of an individual value. Separates product strings, event ID’s, or other values.
    ;	Semi-colon. Represents the end of an individual value in product_list. Separates fields within a single product string.
    =	Equals sign. Assigns a value to an event in product_list.
    ^	Caret. Escapes characters when sent as part of data collection.

    ^,	The value ‘,’ was sent during data collection, escaped by Adobe.
    ^;	The value ‘;’ was sent during data collection, escaped by Adobe.
    ^=	The value ‘=’ was sent during data collection, escaped by Adobe.
    ^^	The value ‘^’ was sent during data collection, escaped by Adobe.
     */

    val itemSeparatorPattern = Pattern.compile("(?<!\\^),")
    val fieldSeparatorPattern = Pattern.compile("(?<!\\^);")
    val pipeSeparatorPattern = Pattern.compile("(?<!\\^)\\|")
    val assignmentPattern = Pattern.compile("(?<!\\^)=")

    def parseProducts(input: String): List[Product] = {
      val items = itemSeparatorPattern.split(input)
      items.map(parseProduct).toList
    }

    //Pattern.compile(regex).matcher(this).replaceAll(replacement);
    val escapedItemSeparatorPattern = Pattern.compile("\\^,")
    val escapedFieldSeparatorPattern = Pattern.compile("\\^;")
    val escapedPipeSeparatorPattern = Pattern.compile("\\^\\|")
    val escapedAssignmentPattern = Pattern.compile("\\^=")
    val escapedEscapePattern = Pattern.compile("\\^\\^")

    def unescape(input: String): String = {

      val unescapePatternsAndReplacements = List(
        (escapedItemSeparatorPattern, ","),
        (escapedFieldSeparatorPattern, ";"),
        (escapedPipeSeparatorPattern, "|"),
        (escapedAssignmentPattern, "="),
        (escapedEscapePattern,  "\\^")
      )

      unescapePatternsAndReplacements
        .foldLeft(input)((x, patternAndReplacement) => patternAndReplacement._1.matcher(x).replaceAll(patternAndReplacement._2))
    }

    def parseProduct(input: String): Product = {
      val fields = fieldSeparatorPattern.split(input, 6)
      Product(
        name = unescape(fields(1)),
        category = noneIfEmpty(unescape(fields(0))),
        quantity = if (fields.length >= 3) noneIfEmpty(unescape(fields(2))) else None,
        price = if (fields.length >= 4) noneIfEmpty(unescape(fields(3))) else None,
        events = if (fields.length >= 5) parseEvents(fields(4)) else None,
        evars = if (fields.length >= 6) parseEvars(fields(5)) else None)
    }

    def noneIfEmpty(input: String): Option[String] = if (input.length == 0) None else Some(input)

    def parseEvents(input: String): Option[List[(String, String)]] = {
      if (input.length == 0) {
        None
      }
      else {
        val events = pipeSeparatorPattern.split(input)
        Some(events.map(parseAssignment).toList)
      }
    }

    def parseEvars(input: String): Option[List[(String, String)]] = {
      if (input.length == 0) {
        None
      }
      else {
        val evars = pipeSeparatorPattern.split(input)
        Some(evars.map(parseAssignment).toList)
      }
    }

    def parseAssignment(input: String): (String, String) = {
      val assignmentParts = assignmentPattern.split(input, 2)
      (unescape(assignmentParts(0)), unescape(assignmentParts(1)))
    }
  }
}
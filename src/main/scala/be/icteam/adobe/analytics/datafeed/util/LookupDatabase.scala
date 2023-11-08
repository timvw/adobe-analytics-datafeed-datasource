package be.icteam.adobe.analytics.datafeed.util

import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.rocksdb.{Options, RocksDB}

import java.io.{File, FileInputStream}
import java.nio.file.Files

case class LookupDatabase[T](lookupFile: File, buildLookupValue: Array[String] => T, serialize: T => Array[Byte], deserialize: Array[Byte] => T) extends AutoCloseable {

  def get(key: Array[Byte]): T = {
    val bytes = lookupFileDb.get(key)
    deserialize(bytes)
  }

  def set(key: Array[Byte], value: T): Unit = {
    val bytes = serialize(value)
    lookupFileDb.put(key, bytes)
  }

  protected lazy val tsvParser: TsvParser = {
    val tsvParserSettings = new TsvParserSettings
    new TsvParser(tsvParserSettings)
  }

  protected val lookupFileDb: RocksDB = {
    RocksDB.loadLibrary()

    val options = new Options()
      .setCreateIfMissing(true)
      .setUseDirectReads(true)

    val lookupFileDbDir = Files.createTempDirectory(s"lookups")
    RocksDB.open(options, lookupFileDbDir.toString)
  }

  import scala.collection.JavaConverters._
  val lookupStream = new FileInputStream(lookupFile)
  tsvParser.iterate(lookupStream).iterator().asScala.foreach(x => {
    val key = x.head.getBytes
    val value = buildLookupValue(x.tail)
    set(key, value)
  })
  lookupStream.close()

  override def close(): Unit = {
    lookupFileDb.close()
  }
}

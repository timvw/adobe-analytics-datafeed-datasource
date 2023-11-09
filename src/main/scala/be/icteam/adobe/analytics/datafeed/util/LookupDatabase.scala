package be.icteam.adobe.analytics.datafeed.util

import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.rocksdb.{Options, RocksDB}

import java.io.{File, FileInputStream}
import java.nio.file.Files

case class LookupDatabase(lookupFile: File, buildKeyAndValue: Array[String] => (Array[Byte], Array[Byte])) extends AutoCloseable {

  def get(key: Array[Byte]): Array[Byte] = {
    lookupFileDb.get(key)
  }

  def set(key: Array[Byte], value: Array[Byte]): Unit = {
    lookupFileDb.put(key, value)
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
    val (key, value) = buildKeyAndValue(x)
    set(key, value)
  })
  lookupStream.close()

  override def close(): Unit = {
    lookupFileDb.close()
  }
}

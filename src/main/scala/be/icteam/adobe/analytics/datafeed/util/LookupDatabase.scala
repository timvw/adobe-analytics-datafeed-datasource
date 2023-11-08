package be.icteam.adobe.analytics.datafeed.util

import com.univocity.parsers.tsv.{TsvParser, TsvParserSettings}
import org.rocksdb.{Options, RocksDB}

import java.io.{File, FileInputStream}
import java.nio.file.Files

case class LookupDatabase(lookupFile: File) extends AutoCloseable {

  def get(key: Array[Byte]): Array[Byte] = lookupFileDb.get(key)

  private lazy val lookupFileDb: RocksDB = load()

  private def load() = {
    RocksDB.loadLibrary()

    val options = new Options()
      .setCreateIfMissing(true)
      .setUseDirectReads(true)

    val lookupFileDbDir = Files.createTempDirectory(s"lookups")
    val lookupFileDb = RocksDB.open(options, lookupFileDbDir.toString)

    import scala.collection.JavaConverters._

    val lookupStream = new FileInputStream(lookupFile)
    val tsvParserSettings = new TsvParserSettings
    tsvParserSettings.setMaxColumns(10)
    val tokenizer = new TsvParser(tsvParserSettings)
    tokenizer.iterate(lookupStream).iterator().asScala.foreach(x => {
      lookupFileDb.put(x(0).getBytes, x(1).getBytes)
    })
    lookupStream.close()
    lookupFileDb
  }

  override def close(): Unit = {
    lookupFileDb.close()
  }
}

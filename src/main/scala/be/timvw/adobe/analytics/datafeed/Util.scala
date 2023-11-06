package be.timvw.adobe.analytics.datafeed

import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.stream.Collectors

case object Util {

  def readTextFile(fs: FileSystem, path: Path): String = {
    val dataInputStream = fs.open(path)
    try {
      readEntireStream(dataInputStream)
    } finally {
      dataInputStream.close()
    }
  }

  def readEntireStream(is: InputStream, close: Boolean = true): String = {
    val reader = new BufferedReader(new InputStreamReader(is))
    try {
      reader.lines.collect(Collectors.joining(System.lineSeparator))
    } finally {
      if (close) {
        reader.close()
      }
    }
  }
}

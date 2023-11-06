package be.icteam.adobe.analytics.datafeed

import org.apache.hadoop.fs.Path

case class DataFile(path: Path, md5: String, size: String, recordCount: Long = 1) {
  def filesetName: String = DataFile.filesetName(this)
}

case object DataFile {
  def filesetName(dataFile: DataFile): String = {
    dataFile.path.getName.substring(dataFile.path.getName.indexOf("-")+1).replace(".tsv.gz", "")
  }
}

package be.icteam.adobe.analytics.datafeed

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.execution.datasources.PathFilterStrategy

import java.io.File
import java.util.regex.Pattern



case class ManifestFile(lookupFiles: List[LookupFile], dataFiles: List[DataFile]) {
  def write(conf: Configuration, folder: Path): Unit = ManifestFile.write(conf, this, folder)

  def extractLookupFiles(conf: Configuration): Map[String, File] = ManifestFile.extractLookupFiles(conf, this)

  def filesetName = ManifestFile.filesetName(this)
}

case object ManifestFile {

  def getManifestFilesInPath(conf: Configuration, path: Path, pathFilters: Seq[PathFilterStrategy]): List[ManifestFile] = {

    val fs = FileSystem.get(path.toUri, conf)

    val manifestFilePaths =
      if (fs.getFileStatus(path).isFile)
        List(path)
      else {
        val fileIterator = fs.listFiles(path, false)
        Iterator.continually(fileIterator)
          .takeWhile(_.hasNext)
          .map(_.next())
          .filter(x => x.getPath.getName.endsWith(".txt"))
          .filter(x => pathFilters.forall(_.accept(x)))
          .map(_.getPath)
          .toList
      }

    manifestFilePaths.map(x => ManifestFile.parse(conf, x))
  }

  def parse(conf: Configuration, manifestFilePath: Path): ManifestFile = {
    val fs = FileSystem.get(manifestFilePath.toUri, conf)
    val manifestDirectory = manifestFilePath.getParent
    try {
      val manifestText = Util.readTextFile(fs, manifestFilePath)
      val lookupFiles = parseLookupFiles(manifestText, manifestDirectory)
      val dataFiles = parseDataFiles(manifestText, manifestDirectory)
      ManifestFile(lookupFiles, dataFiles)
    } finally {
      fs.close()
    }
  }

  final val lookupFilePattern = Pattern.compile("^Lookup-File: (.*?)\nMD5-Digest: (.*?)\nFile-Size: (.*?)$", Pattern.MULTILINE)

  def parseLookupFiles(manifestText: String, manifestDirectory: Path): List[LookupFile] = {
    val matcher = lookupFilePattern.matcher(manifestText)
    Iterator
      .continually(matcher)
      .takeWhile(matcher => matcher.find())
      .map(matcher => LookupFile(new Path(manifestDirectory, matcher.group(1)), matcher.group(2), matcher.group(3)))
      .toList
  }

  final val dataFilePattern = Pattern.compile("^Data-File: (.*?)\nMD5-Digest: (.*?)\nFile-Size: (.*?)$", Pattern.MULTILINE)

  def parseDataFiles(manifestText: String, manifestDirectory: Path): List[DataFile] = {
    val matcher = dataFilePattern.matcher(manifestText)

    Iterator
      .continually(matcher)
      .takeWhile(matcher => matcher.find())
      .map(matcher => DataFile(new Path(manifestDirectory, matcher.group(1)), matcher.group(2), matcher.group(3)))
      .toList
  }

  def extractLookupFiles(conf: Configuration, manifestFile: ManifestFile): Map[String, File] = {
    manifestFile.lookupFiles.flatMap(x => LookupFile.extract(conf, x.path)).toMap
  }

  def filesetName(manifestFile: ManifestFile): String = {
    val maybeLookupFile = manifestFile.lookupFiles.headOption
    val maybeDataFile = manifestFile.dataFiles.headOption
    (maybeLookupFile, maybeDataFile) match {
      case (Some(lookupFile), _) => lookupFile.filesetName
      case (_, Some(dataFile)) => dataFile.filesetName
      case (None, None) => throw new Exception("impossible to infer fileset name when there are no fils")
    }
  }

  def write(conf: Configuration, manifestFile: ManifestFile, manifestFolder: Path): Path = {
    val manifestFilename = manifestFile.lookupFiles.head.path.toString.replace("-lookup_data.tar.gz", ".txt")
    val manifestFilePath = new Path(manifestFolder, manifestFilename)
    val file = FileSystem.get(conf).create(manifestFilePath)
    val content = new StringBuilder()
    content.append(s"Datafeed-Manifest-Version: 1.0\nLookup-Files: ${manifestFile.lookupFiles.size}\nData-Files: ${manifestFile.dataFiles.size}\nTotal-Records: ${manifestFile.dataFiles.map(_.recordCount).sum}")
    manifestFile.lookupFiles.foldLeft(content)((content, file) => content.append(s"\n\nLookup-File: ${file.path.toString}\nMD5-Digest: ${file.md5}\nFile-Size: ${file.size}"))
    manifestFile.dataFiles.foldLeft(content)((content, file) => content.append(s"\n\nData-File: ${file.path.toString}\nMD5-Digest: ${file.md5}\nFile-Size: ${file.size}\nRecord-Count: ${file.recordCount}"))
    file.write(content.toString().getBytes("UTF-8"))
    file.close
    manifestFilePath
  }



  def generateManifestsFromHitdata(conf: Configuration, hitdataFolder: Path, lookupdataFolder: Path): List[ManifestFile] = {

    // this is help in the situation where the manifest file is lost, but there are still data files...
    val fs = FileSystem.get(hitdataFolder.toUri, conf)

    val hitdataFileIterator = fs.listFiles(hitdataFolder, true)
    val hitdataFilePaths: List[Path] = Iterator.continually(hitdataFileIterator)
      .takeWhile(_.hasNext)
      .map(_.next())
      .map(_.getPath)
      .filter(x => x.getName.endsWith(".tsv.gz"))
      .toList

    val lookupFileIterator = fs.listFiles(lookupdataFolder, true)
    val lookupFilePaths = Iterator.continually(lookupFileIterator)
      .takeWhile(_.hasNext)
      .map(_.next())
      .map(_.getPath)
      .filter(x => x.getName.endsWith("-lookup_data.tar.gz"))
      .toList

    val lookupFilesByFileset = lookupFilePaths.map(x => {
      val key = x.getName.replace("-lookup_data.tar.gz", "")
      (key, x)
    }).toMap

    val manifestFiles = hitdataFilePaths.flatMap(hitdataFilePath => {
      val key = hitdataFilePath.getName.replace("01-", "").replace(".tsv.gz", "")
      lookupFilesByFileset.get(key)
        .map(lookupFilePath => {
          val lookupFiles = List(LookupFile(lookupFilePath, "", ""))
          val dataFiles = List(DataFile(hitdataFilePath, "", ""))
          ManifestFile(lookupFiles, dataFiles)
        })
    })

    manifestFiles
  }
}

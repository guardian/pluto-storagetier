package com.gu.multimedia.storagetier.utils

object FilenameSplitter {
  private val splitter = "(.*)(\\.[^\\.]+)".r

  def apply(n:String) = n match {
    case splitter(filename,xtn)=>(filename, Some(xtn))
    case _=>(n, None)
  }

  /**
   * inserts an extra string into the filename, between the filename and the extension
   * @param filename original filename, including extension
   * @param partToInsert the part to insert
   * @return the resulting filename
   */
  def insertPostFilename(filename:String, partToInsert:String) = {
    val parts = FilenameSplitter.apply(filename)

    parts._1 + partToInsert + parts._2.getOrElse("")
  }
}

package com.gu.multimedia.storagetier.vidispine

import org.slf4j.LoggerFactory

import java.time.ZonedDateTime

case class VSShapeFile(
                      id: String,
                      path: String,
                      uri: Seq[String],
                      state: String,
                      size: Long,
                      hash: Option[String],
                      timestamp: String,  //sure, this should really be a ZonedDateTime. But since we are not using the
                      //field and it can cause parsing issues, keeping it as a String for the time being.
                      refreshFlag: Int,
                      storage: String,
                      ) {
  def sizeOption = if(size == -1) None else Some(size)
}

/**
 * simplified Component stanza of the VSShapeDocument, containing just what we are interested in
 * @param id component ID
 * @param file list of VSShapeFile instances
 */
case class SimplifiedComponent(id:String, file:Seq[VSShapeFile])

case class ShapeDocument(
                        id: String,
                        created: String,
                        essenceVersion: Int,
                        tag: Seq[String],
                        mimeType: Seq[String],
                        containerComponent: SimplifiedComponent,
                        audioComponent: Option[Seq[SimplifiedComponent]],
                        videoComponent: Option[Seq[SimplifiedComponent]]
                        ) {
  private val logger = LoggerFactory.getLogger(getClass)

  def getLikelyFile:Option[VSShapeFile] = {
    val audioFiles = audioComponent.getOrElse(Seq.empty[SimplifiedComponent]).flatMap(_.file)
    val videoFiles = videoComponent.getOrElse(Seq.empty[SimplifiedComponent]).flatMap(_.file)
    val allComponentFiles = containerComponent.file ++ audioFiles ++ videoFiles
    val fileIdMap = allComponentFiles.foldLeft(Map[String, VSShapeFile]())((acc, elem)=>acc + (elem.id->elem))
    if(fileIdMap.size>1) {
      logger.warn(s"Shape $id with tag(s) ${tag.mkString(";")} has got ${fileIdMap.size} different files attached to it, expected one. Using the first.")
    }
    fileIdMap.headOption.map(_._2)
  }
}

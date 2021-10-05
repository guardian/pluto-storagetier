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
                      timestamp: ZonedDateTime,
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
                        created: ZonedDateTime,
                        essenceVersion: Int,
                        tag: Seq[String],
                        mimeType: Seq[String],
                        containerComponent: SimplifiedComponent,
                        audioComponent: Seq[SimplifiedComponent],
                        videoComponent: Seq[SimplifiedComponent]
                        ) {
  private val logger = LoggerFactory.getLogger(getClass)

  def getLikelyFile:Option[VSShapeFile] = {
    val allComponentFiles = containerComponent.file ++ audioComponent.flatMap(_.file) ++ videoComponent.flatMap(_.file)
    val fileIdMap = allComponentFiles.foldLeft(Map[String, VSShapeFile]())((acc, elem)=>acc + (elem.id->elem))
    if(fileIdMap.size>1) {
      logger.warn(s"Shape $id with tag(s) ${tag.mkString(";")} has got ${fileIdMap.size} different files attached to it, expected one. Using the first.")
    }
    fileIdMap.headOption.map(_._2)
  }
}

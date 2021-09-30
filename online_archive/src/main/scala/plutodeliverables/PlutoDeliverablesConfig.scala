package plutodeliverables

/**
 * Configuration for uploading deliverables
 * @param uploadBasePath base path (key) at which they should be uploaded.
 * @param presevePathParts number of path parts to preseve. If this is set to 2, the filename and its immediate parent directory will be kept.
 */
case class PlutoDeliverablesConfig(uploadBasePath:String, presevePathParts:Int)

object PlutoDeliverablesConfig extends ((String, Int)=>PlutoDeliverablesConfig) {
  /**
   * default constructor for PlutoDeliverablesConfig. Takes the upload base path from the environment variable DELIVERABLES_UPLOAD_BASEPATH
   * and defaults to `Masters` if that is not found. Keeps the immediate parent directory only.
   * @return
   */
  def apply() = new PlutoDeliverablesConfig(sys.env.getOrElse("DELIVERABLES_UPLOAD_BASEPATH","Deliverables"), 2)
}

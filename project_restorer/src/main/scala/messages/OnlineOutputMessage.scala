package messages

case class OnlineOutputMessage(mediaTier: String,
                               projectId: Int,
                               filePath: String,
                               itemId: String,
                               nearlineVersionId: String,
                               nearlineversion: Int,
                               mediaCategory: String)

/**
 * This is raised in a callchain to tell a root exception handler _not_ to catch this error for a retry
 *
 * @param msg error message
 */
class BailOutExceptionMR(msg:String) extends Exception(msg)

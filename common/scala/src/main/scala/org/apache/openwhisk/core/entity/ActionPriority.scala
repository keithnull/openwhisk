package org.apache.openwhisk.core.entity

sealed trait ActionPriority
object ActionPriority {
  case object High extends ActionPriority
  case object Low extends ActionPriority
  case object Normal extends ActionPriority

  def fromString(s: String = ""): ActionPriority = {
    s.toLowerCase match {
      case "high" => High
      case "low"  => Low
      case _      => Normal
    }
  }
}

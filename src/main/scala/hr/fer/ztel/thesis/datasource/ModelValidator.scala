package hr.fer.ztel.thesis.datasource

object ModelValidator extends Serializable {

  def isParsableUserItemRecord(parts: Array[String]): Boolean = {

    // user, date, user, quantity
    if (parts.length != 4) return false

    try {
      parts(0).toInt
      //parts(1) is date but it's not used
      parts(2).toInt
      parts(3).toDouble
      true
    }

    catch {
      case (_: NumberFormatException) =>
        false
    }

  }

  def isParsableItemItemRecord(parts: Array[String]): Boolean = {

    // (item1, item2, a, b, c, d)
    if (parts.length != 6) return false

    try {
      parts.map(_.toInt)
      true
    }

    catch {
      case (_: NumberFormatException) => false
    }

  }

}
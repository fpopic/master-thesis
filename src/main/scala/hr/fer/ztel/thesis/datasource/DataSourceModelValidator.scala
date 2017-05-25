package hr.fer.ztel.thesis.datasource

object DataSourceModelValidator extends Serializable {

  def isParsableCustomerItemRecord(parts : Array[String]) : Boolean = {

    // customerId, date, userId, quantity
    if (parts.length != 4) {
      return false
    }

    try {
      parts(0).toInt
      //parts(1) is date but it's not used
      parts(2).toInt
      parts(3).toDouble
      true
    }

    catch {
      case (_ : NumberFormatException) =>
        false
    }

  }

  def isParsableItemItemRecord(parts : Array[String]) : Boolean = {

    // (itemId1, itemId2, a, b, c, d)
    if (parts.length != 6) return false

    try {
      parts.map(_.toInt)
      true
    }

    catch {
      case (_ : NumberFormatException) => false
    }

  }

}
package hr.fer.ztel.thesis.datasource

object DataSourceModelValidator extends Serializable {

  def isParsableCustomerItemRecord(parts : Array[String]) : Boolean = {

    // customerId, date, userId, quantity //todo
    if (parts.length != 3) { //todo
      return false
    }

    try {
      parts(0).toInt
      //parts(1) is date but it's not used //todo
      parts(1).toInt//todo
      parts(2).toDouble//todo
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
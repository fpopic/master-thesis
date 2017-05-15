package hr.fer.ztel.dipl.model

////yyyanonCustID,date,anonItemID,quantity
////29'154'707 records (all ok)
case class CustomerItem(customerId : Int, itemId : Int)

case class CustomerItemRecord(customerId : Int, itemId : Int, quantity : Double)

object CustomerItemRecord {

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

  def isBought(quantity : Double) : Double = if (quantity >= 1) 1.0 else 0.0

}
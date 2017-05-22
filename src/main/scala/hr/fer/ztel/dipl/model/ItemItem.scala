package hr.fer.ztel.dipl.model

//anonID1,anonID2,a,b,c,d
//71'930'771 records

// itemitem = 67052 x 67052 x 4B = 17 956 000 000 B ~ 18  GB
// one row  = 1     x 67052 x 4B = 268 208          ~ 268 kB

case class ItemItemRecord(item1 : Int, item2 : Int, a : Int, b : Int, c : Int, d : Int)

object ItemItemRecord {

  def isParsableItemItemRecord(parts : Array[String]) : Boolean = {

    // (itemId1, itemId2, a, b, c, d)
    if (parts.length != 6) return false

    try {
      parts.map(_.toInt)
      true
    } catch {
      case (_ : NumberFormatException) => false
    }

  }
}

case class ItemItemEntry(item1 : Int, item2 : Int, similarity : Double)
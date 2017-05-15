package hr.fer.ztel.dipl.model

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object DataSource extends Serializable {

  def createItemItemMatrix(path : String, measure : ItemPairSimiliarityMeasure)
    (implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import ItemItemRecord.isParsableItemItemRecord
    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(t => (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt))
      .flatMap {
        case (itemId1, itemId2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // associative item-item entries (x, y) (y, x)
          Seq((itemId1, (itemId2, similarity)), (itemId2, (itemId1, similarity)))
      }
      .rdd
      .groupByKey
      .map {
        case (itemIndex, itemVector) => (itemIndex, itemVector.toMap)
      }
  }

  def createCustomerItemMatrix(path : String)
    (implicit spark : SparkSession) : RDD[(Int, Set[Int])] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => CustomerItemRecord(customerId.toInt, itemId.toInt, quantity.toDouble)
      }
      .groupBy('customerId, 'itemId)
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[CustomerItem]
      .map {
        case CustomerItem(customerId, itemId) => (customerId, itemId)
      }
      .rdd
      .groupByKey
      .map {
        case (customerId, customerVector) => (customerId, customerVector.toSet)
      }
  }

  def createItemCustomerMatrix(path : String)
    (implicit spark : SparkSession) : RDD[(Int, Set[Int])] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
    import spark.implicits._

    spark.read
      .textFile("src/main/resources/transactions_10.csv")
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => ItemCustomerRecord(itemId.toInt, customerId.toInt, quantity.toDouble)
      }
      .groupBy('customerId, 'itemId)
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[ItemCustomer]
      .rdd
      .map {
        case ItemCustomer(itemId, customerId) => (itemId, customerId)
      }
      .groupByKey
      .map {
        case (itemId, itemVector) => (itemId, itemVector.toSet)
      }
  }

}

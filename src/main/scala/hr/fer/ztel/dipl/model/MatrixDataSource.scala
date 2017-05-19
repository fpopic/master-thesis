package hr.fer.ztel.dipl.model

import hr.fer.ztel.dipl.ml.ItemPairSimiliarityMeasure
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object MatrixDataSource extends Serializable {

  def createItemItemMatrix(path : String, measure : ItemPairSimiliarityMeasure, partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import ItemItemRecord.isParsableItemItemRecord
    import spark.implicits._

    val customerItemRDD = spark.read
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

    if (partitioner.isDefined)
      customerItemRDD
        .groupByKey(partitioner.get)
        .mapValues(itemVector => itemVector.toMap)

    else
      customerItemRDD
        .groupByKey
        .mapValues(itemVector => itemVector.toMap)
  }

  def createItemCustomerMatrix(path : String, partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Set[Int])] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
    import spark.implicits._

    val itemCustomerRDD : RDD[(Int, Int)] = spark.read
      .textFile(path)
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

    if (partitioner.isDefined)
      itemCustomerRDD
        .groupByKey(partitioner = partitioner.get)
        .mapValues(customerVector => customerVector.toSet)

    else
      itemCustomerRDD
        .groupByKey
        .mapValues(customerVector => customerVector.toSet)
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
      .mapValues(itemVector => itemVector.toSet)
  }

  def collectCustomerIndexes(df : DataFrame) : Map[Int, Int] = {
    //    d

    //    val itemIndexes = (itemItemRecords.map (_.item1Id).distinct union itemItemRecords.map (_.item2Id).distinct).distinct

    //    val itemIndexes = (itemItemRecords.select ('item1Id).distinct union itemItemRecords.select ('item2Id).distinct).distinct
    Map.empty[Int, Int]
  }

}
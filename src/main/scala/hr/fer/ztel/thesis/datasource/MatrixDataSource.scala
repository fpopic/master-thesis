package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.datasource.ModelValidator.{isParsableCustomerItemRecord, isParsableItemItemRecord}
import hr.fer.ztel.thesis.ml.ItemPairSimilarityMeasure
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object MatrixDataSource extends Serializable {

  def createItemItemMatrix(path : String, measure : ItemPairSimilarityMeasure,
    partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import spark.implicits._

    val itemItemRDD = spark.read
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

    val itemVectors =
      if (partitioner.isDefined)
        itemItemRDD.groupByKey(partitioner.get)
      else
        itemItemRDD.groupByKey

    if (measure.normalize)
      itemVectors.mapValues(itemVector => {
        val norm = itemVector.foldLeft(0.0)((sum, t) => sum + t._2)
        if (norm != 0)
          itemVector.map { case (item, similarity) => (item, similarity / norm) }.toMap
        else
          itemVector.toMap
      })

    else
      itemVectors.mapValues(itemVector => itemVector.toMap)
  }

  def createBoughtItemItemMatrix(path : String, measure : ItemPairSimilarityMeasure,
    boughtItems : Broadcast[Array[Int]], partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import spark.implicits._

    val itemItemRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .mapPartitions {
        partition => {
          val localBoughtItems = boughtItems.value.toSet
          partition.flatMap(
            t => {
              val (itemId1, itemId2, a, b, c, d) =
                (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt)
              val similarity = measure.compute(a, b, c, d)
              // associative item-item entries (x, y) (y, x)
              val array = ArrayBuffer.empty[(Int, (Int, Double))]
              if (localBoughtItems contains itemId1)
                array += (itemId1 -> (itemId2, similarity))
              if (localBoughtItems contains itemId1)
                array += (itemId2 -> (itemId1, similarity))
              if (array.nonEmpty) array else None
            }
          )
        }
      }
      .rdd

    val itemVectors =
      if (partitioner.isDefined)
        itemItemRDD.groupByKey(partitioner.get)
      else
        itemItemRDD.groupByKey

    if (measure.normalize)
      itemVectors.mapValues(itemVector => {
        val norm = itemVector.foldLeft(0.0)((sum, t) => sum + t._2)
        if (norm != 0)
          itemVector.map { case (item, similarity) => (item, similarity / norm) }.toMap
        else
          itemVector.toMap
      })

    else
      itemVectors.mapValues(itemVector => itemVector.toMap)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def createItemCustomerMatrix(path : String, partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Array[Int])] = {

    import spark.implicits._

    val itemCustomerRDD : RDD[(Int, Int)] = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => (itemId.toInt, customerId.toInt, quantity.toDouble)
      }
      .toDF("item", "customer", "quantity")
      .groupBy("item", "customer")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd

    if (partitioner.isDefined)
      itemCustomerRDD
        .groupByKey(partitioner = partitioner.get)
        .mapValues(customerVector => customerVector.toArray)

    else
      itemCustomerRDD
        .groupByKey
        .mapValues(customerVector => customerVector.toArray)
  }

  def createCustomerItemMatrix(path : String)
    (implicit spark : SparkSession) : RDD[(Int, Array[Int])] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => (customerId.toInt, itemId.toInt, quantity.toDouble)
      }
      .toDF("customer", "item", "quantit")
      .groupBy("customer", "item")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd
      .groupByKey
      .mapValues(itemVector => itemVector.toArray)
  }

}
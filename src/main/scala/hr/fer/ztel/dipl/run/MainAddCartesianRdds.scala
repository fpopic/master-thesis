package hr.fer.ztel.dipl.run

import hr.fer.ztel.dipl.model.CustomerItemRecord._
import hr.fer.ztel.dipl.model.ItemItemRecord._
import hr.fer.ztel.dipl.model.SparseVectorAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


object MainAddCartesianRdds {

  def main(args : Array[String]) : Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    import spark.implicits._

    //region ItemItem

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemRecords = spark.read
      .textFile("src/main/resources/item_matrix_10.csv")
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(t => (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt))

    val itemItemMatrix : RDD[(Int, Map[Int, Double])] = itemItemRecords.rdd
      .flatMap {
        case (itemId1, itemId2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // associative item-item entries (x, y) (x, y)
          Seq((itemId1, (itemId2, similarity)), (itemId2, (itemId1, similarity)))
      }
      .groupByKey
      .map {
        case (itemIndex : Int, itemVector : Seq[(Int, Double)]) => (itemIndex, itemVector.toMap ))
      }

    //endregion

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val customerItemRecords = spark.read
      .textFile("src/main/resources/transactions_10.csv")
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => CustomerItemRecord(customerId.toInt, itemId.toInt, quantity.toFloat)
      }
      .groupBy('customerId, 'itemId).agg("quantity" -> "sum")
      .withColumnRenamed("sum(quantity)", "quantity")
      .where('quantity >= 1.0)
      .drop('quantity)
      .as[CustomerItem]

    val customerItemMatrix : collection.Map[Int, Set[Int]] = customerItemRecords.rdd
      .map {
        case CustomerItem(customerId, itemId) => (customerId, itemId)
      }
      .groupByKey
      .map {
        case (customerId, customerVector) => (customerId, customerVector.toSet)
      }
      .collectAsMap()

    val customerItemMatrixBroadcasted = spark.sparkContext.broadcast(customerItemMatrix)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val partialVectors : collection.Map[Int, Map[Int, Double]] =
      for {
        (customerId, customerVector) <- customerItemMatrixBroadcasted.value
        localItemRows = itemItemMatrix.filter {
          case (itemId, _) => customerVector contains itemId
        }
        if !localItemRows.isEmpty
      } yield localItemRows.reduce {
        case ((_, itemVector1), (_, itemVector2)) => (customerId, add(itemVector1, itemVector2))
      }

    val partialVectorsRDD = spark.sparkContext.makeRDD(partialVectors.toSeq)

    val N = 5

    // reduce at workers
    val recommendationMatrix = partialVectorsRDD
      .reduceByKey(add(_, _), partialVectors.size)
      .map {
        case (customerId, utilities) => (customerId, utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))
      }

    recommendationMatrix.foreach(println)

  }

}
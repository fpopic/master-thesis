package hr.fer.ztel.dipl.run

import hr.fer.ztel.dipl.model.CustomerItemRecord._
import hr.fer.ztel.dipl.model.ItemItemRecord._
import hr.fer.ztel.dipl.model.{CosineSimiliarityMeasure, CustomerItem, CustomerItemRecord, ItemPairSimiliarityMeasure}
import hr.fer.ztel.dipl.model.SparseVectorAlgebra._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MainDotCartesianRddWithBroadcast {

  def main(args : Array[String]) : Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    import spark.implicits._

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
          // associative item-item entries (x, y) (y, x)
          Seq((itemId1, (itemId2, similarity)), (itemId2, (itemId1, similarity)))
      }
      .groupByKey
      .map {
        case (itemIndex, itemVector) => (itemIndex, itemVector.toMap)
      }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val customerItemRecords = spark.read
      .textFile("src/main/resources/transactions_10.csv")
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => CustomerItemRecord(customerId.toInt, itemId.toInt, quantity.toFloat)
      }
      .groupBy('customerId, 'itemId).agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[CustomerItem]

    val customerItemMatrix : Array[(Int, Set[Int])] = customerItemRecords.rdd
      .map {
        case CustomerItem(customerId, itemId) => (customerId, itemId)
      }
      .groupByKey
      .map {
        case (customerId, customerVector) => (customerId, customerVector.toSet)
      }
      .collect

    val customerItemMatrixBroadcasted = spark.sparkContext.broadcast(customerItemMatrix)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val N = 5 // top n recommendations

    val recommendations = itemItemMatrix.mapPartitions({ iter =>
      val localCustomerItemMatrix = customerItemMatrixBroadcasted.value
      iter.map {
        case (itemId, itemVector) => localCustomerItemMatrix.map {
          case (customerId, customerVector) => (customerId, (itemId, dot(customerVector, itemVector)))
        }.toSeq
      }.flatten
    }, preservesPartitioning = true)
      .groupByKey
      .map {
        case (customerId, utilities) => (customerId, utilities.toSeq.sortBy(_._2).takeRight(N).map(_._1))
      }

    println(recommendations.count)

  }

}
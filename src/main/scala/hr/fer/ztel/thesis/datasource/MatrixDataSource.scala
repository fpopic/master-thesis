package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.datasource.ModelValidator._
import hr.fer.ztel.thesis.measure.ItemPairSimilarityMeasure
import hr.fer.ztel.thesis.sparse_linalg.SparseVectorOperators._
import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable.ArrayBuffer

object MatrixDataSource extends Serializable {

  def readItemItemMatrix(path: String, measure: ItemPairSimilarityMeasure,
    normalizeRows: Boolean, partitioner: Option[Partitioner] = None)
    (implicit spark: SparkSession): RDD[(Int, Map[Int, Double])] = {

    import spark.implicits._

    val itemItemEntriesRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(t => (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt))
      .flatMap { case (itemId1, itemId2, a, b, c, d) =>
        val similarity = measure.compute(a, b, c, d)
        // commutative entries (x, y) (y, x)
        Seq((itemId1, (itemId2, similarity)), (itemId2, (itemId1, similarity)))
      }
      .rdd

    val itemItemMatrixRDD =
      if (normalizeRows)
        itemItemEntriesRDD
          .groupByKey
          .mapValues(itemVector => normalize(itemVector.toArray))
          .flatMap { case (item, itemRow) =>
            itemRow.map { case (otherItem, similarity) =>
              (otherItem, (item, similarity)) // key is col index (rows != cols)
            }
          }
      else itemItemEntriesRDD // key is row index (simetric matrix rows == cols)

    val groupedItemMatrixRDD =
      if (partitioner.isDefined)
        itemItemMatrixRDD.groupByKey(partitioner.get)
      else
        itemItemMatrixRDD.groupByKey

    groupedItemMatrixRDD.mapValues(_.toMap)
  }

  def readBoughtItemItemMatrix(path: String, measure: ItemPairSimilarityMeasure,
    normalizeRows: Boolean, boughtItems: Broadcast[Array[Int]], partitioner: Option[Partitioner] = None)
    (implicit spark: SparkSession): RDD[(Int, Map[Int, Double])] = {

    import spark.implicits._

    val itemItemEntriesRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .mapPartitions { partition => {
        val localBoughtItems = boughtItems.value.toSet
        partition.flatMap(t => {
          val (item1, item2, a, b, c, d) =
            (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt)
          val similarity = measure.compute(a, b, c, d)
          // commutative item-item entries (x, y) (y, x)
          val array = ArrayBuffer.empty[(Int, (Int, Double))]
          if (localBoughtItems contains item1)
            array += (item1 -> (item2, similarity))
          if (localBoughtItems contains item1)
            array += (item2 -> (item1, similarity))
          if (array.nonEmpty) array else None
        }
        )
      }
      }
      .rdd

    val itemItemMatrixRDD =
      if (normalizeRows)
        itemItemEntriesRDD
          .groupByKey
          .mapValues(itemVector => normalize(itemVector.toArray))
          .flatMap { case (item, itemRow) =>
            itemRow.map { case (otherItem, similarity) =>
              (otherItem, (item, similarity)) // key is col index (rows != cols)
            }
          }
      else itemItemEntriesRDD // key is row index (simetric matrix rows == cols)

    val groupedItemMatrixRDD =
      if (partitioner.isDefined)
        itemItemMatrixRDD.groupByKey(partitioner.get)
      else
        itemItemMatrixRDD.groupByKey

    groupedItemMatrixRDD.mapValues(_.toMap)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def readItemUserMatrix(path: String, partitioner: Option[Partitioner] = None)
    (implicit spark: SparkSession): RDD[(Int, Array[Int])] = {

    import spark.implicits._

    val itemUserRDD: RDD[(Int, Int)] = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableUserItemRecord(_))
      .map { case Array(user, timestamp, item, quantity) =>
        (item.toInt, user.toInt, quantity.toDouble)
      }
      .toDF("item", "user", "quantity")
      .groupBy("item", "user")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd

    val itemUserRdd =
      if (partitioner.isDefined)
        itemUserRDD.groupByKey(partitioner.get)
      else
        itemUserRDD.groupByKey

    itemUserRdd.mapValues(userVector => userVector.toArray)
  }

  def readUserItemMatrix(path: String)
    (implicit spark: SparkSession): RDD[(Int, Array[Int])] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableUserItemRecord(_))
      .map { case Array(user, timestamp, item, quantity) =>
        (user.toInt, item.toInt, quantity.toDouble)
      }
      .toDF("user", "item", "quantity")
      .groupBy("user", "item")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd
      .groupByKey
      .mapValues(itemVector => itemVector.toArray)
  }

}
package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.datasource.ModelValidator.{isParsableUserItemRecord, isParsableItemItemRecord}
import hr.fer.ztel.thesis.ml.SparseVectorOperators._
import hr.fer.ztel.thesis.ml.{ItemPairSimilarityMeasure, SparseVectorOperators}
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MatrixEntryDataSource extends Serializable {

  def readUserItemEntries(path: String)
    (implicit spark: SparkSession): RDD[MatrixEntry] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableUserItemRecord(_))
      .map { case Array(user, item, quantity) => (user.toInt, item.toInt, quantity.toDouble) }
      .toDF("user", "item", "quantity")
      .groupBy("user", "item")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .as[(Int, Int, Double)]
      .map {
        case (user, item, _) => MatrixEntry(user, item, 1.0)
      }
      .rdd

  }

  def readItemUserEntries(path: String)
    (implicit spark: SparkSession): RDD[MatrixEntry] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableUserItemRecord(_))
      .map { case Array(user, item, quantity) => (item.toInt, user.toInt, quantity.toDouble) }
      .toDF("item", "user", "quantity")
      .groupBy("item", "user")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .as[(Int, Int, Double)]
      .map { case (item, user, _) => MatrixEntry(item, user, 1.0) }
      .rdd
  }

  def readItemItemEntries(path: String, measure: ItemPairSimilarityMeasure)
    (implicit spark: SparkSession): RDD[MatrixEntry] = {

    import spark.implicits._

    val itemItemRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(t => (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt))

    if (measure.normalize)
      itemItemRDD
        .flatMap { case (item1, item2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // commutative item-item entries (x, y) (y, x)
          Seq((item1, (item2, similarity)), (item2, (item1, similarity)))
        }
        .rdd
        .groupByKey
        .mapValues(itemVector => normalize(itemVector.toArray))
        .flatMap { case (item, itemVector) => itemVector.map {
          case (otherItem, similarity) => MatrixEntry(item, otherItem, similarity)
        }
        }

    else
      itemItemRDD
        .flatMap { case (item1, item2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // commutative item-item entries (x, y) (y, x)
          Seq(MatrixEntry(item1, item2, similarity), MatrixEntry(item2, item1, similarity))
        }
        .rdd
  }

}
package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.datasource.DataSourceModelValidator.{isParsableCustomerItemRecord, isParsableItemItemRecord}
import hr.fer.ztel.thesis.ml.ItemPairSimilarityMeasure
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MatrixEntryDataSoruce extends Serializable {

  def readCustomerItemEntries(path : String)
    (implicit spark : SparkSession) : RDD[MatrixEntry] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customer, _, item, quantity) => (customer, item, quantity.toDouble)
      }
      .toDF("customer", "item", "quantity")
      .groupBy("customer", "item")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .as[(Int, Int, Double)]
      .map {
        case (customer, item, _) => MatrixEntry(customer, item, 1.0)
      }
      .rdd

  }

  def readItemCustomerEntries(path : String)
    (implicit spark : SparkSession) : RDD[MatrixEntry] = {

    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customer, _, item, quantity) => (item, customer, quantity.toDouble)
      }
      .toDF("item", "customer", "quantity")
      .groupBy("item", "customer")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .as[(Int, Int, Double)]
      .map {
        case (item, customer, _) => MatrixEntry(item, customer, 1.0)
      }
      .rdd
  }

  def readItemItemEntries(path : String, measure : ItemPairSimilarityMeasure)
    (implicit spark : SparkSession) : RDD[MatrixEntry] = {

    import spark.implicits._

    val itemItemRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(t => (t(0).toInt, t(1).toInt, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt))
      .cache

    if (measure.normalize)
      itemItemRDD
        .flatMap {
          case (item1, item2, a, b, c, d) =>
            val similarity = measure.compute(a, b, c, d)
            // associative item-item entries (x, y) (y, x)
            Seq((item1, (item2, similarity)), (item2, (item1, similarity)))
        }
        .rdd
        .groupByKey
        .mapValues {
          itemVector => {
            val norm = itemVector.foldLeft(0.0)((sum, t) => sum + t._2)
            if (norm != 0.0)
              itemVector.map { case (item, similarity) => (item, similarity / norm) }
            else
              itemVector
          }
        }
        .flatMap {
          case (item, itemVector) => itemVector.map {
            case (otherItem, similarity) => MatrixEntry(item, otherItem, similarity)
          }
        }

    else
      itemItemRDD
        .flatMap {
          case (item1, item2, a, b, c, d) =>
            val similarity = measure.compute(a, b, c, d)
            // associative item-item entries (x, y) (y, x)
            Seq(MatrixEntry(item1, item2, similarity), MatrixEntry(item2, item1, similarity))
        }
        .rdd
  }

}
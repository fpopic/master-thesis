package hr.fer.ztel.thesis.ml.multiplication.inner

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import org.apache.spark.sql.SparkSession

object MainDotCartesianRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "400000000")

    val measure = new CosineSimilarityMeasure

    val itemItemMatrix = createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix = createCustomerItemMatrix("src/main/resources/transactions_10.csv")

    val N = 5 // top n recommendations

    val recommendationMatrix = (customerItemMatrix cartesian itemItemMatrix)
      .map {
        case ((customerId, customerVector), (itemId, itemVector)) => (customerId, (itemId, dot(customerVector, itemVector)))
      }
      .groupByKey
      .mapValues(utilities => utilities.toArray.sortBy(_._2).takeRight(N).map(_._1))

    println(recommendationMatrix.count)

  }

}

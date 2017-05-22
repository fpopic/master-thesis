package hr.fer.ztel.dipl.run.outer

import hr.fer.ztel.dipl.datasource.MatrixDataSource
import hr.fer.ztel.dipl.ml.CosineSimiliarityMeasure
import hr.fer.ztel.dipl.ml.SparseLinearAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MainOuterRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // za lokalno
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure = new CosineSimiliarityMeasure

    val partitioner = Some(new HashPartitioner(32))

    val itemCustomerMatrix : RDD[(Int, Set[Int])] = // (item, customers)
      MatrixDataSource.createItemCustomerMatrix("src/main/resources/transactions_10.csv", partitioner)

    val itemItemMatrix : RDD[(Int, Map[Int, Double])] = // (item, sims)
      MatrixDataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure, partitioner)

    val N = 5

    // join should produce at most 67k tuples in shuffle-u
    val recommendations = itemCustomerMatrix.join(itemItemMatrix, partitioner.get)
      .mapPartitions(preservesPartitioning = true, f = { partition =>
        partition.map {
          // computing partial matrices
          // 1 is added as key to enable reducebykey
          case (itemId, (customerVector, itemVector)) => (1, outer(customerVector, itemVector))
        }
      })
      // combiner used locally too
      .reduceByKey(addM)
      // remove matrix container
      .flatMapValues(m => m)
      .map {
        case (1, ((customerId, itemId), utility)) => (customerId, (itemId, utility))
      }
      .groupByKey
      .mapValues(utilities => utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))

    recommendations.count()

  }
}
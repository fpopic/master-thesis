package hr.fer.ztel.thesis.ml.multiplication.add

import hr.fer.ztel.thesis.datasource.MatrixDataSource
import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseLinearAlgebra._
import hr.fer.ztel.thesis.spark.SparkSessionHolder
import org.apache.spark.HashPartitioner

object MainAddInverseJoinRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSessionHolder.getSession

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "400000000")

    val partitioner = Some(new HashPartitioner(partitions = 32))

    val measure = new CosineSimilarityMeasure

    val itemItemMatrix = createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure, partitioner)

    // tu mozda ne treba Set moze i Seq ako je brze ista
    val itemCustomerMatrix = createItemCustomerMatrix("src/main/resources/transactions_10.csv", partitioner)

    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = (itemItemMatrix join itemCustomerMatrix)
      .flatMap {
        // skupljam retke za customera, nista se ne racuna
        case (_, (itemVector, customerIds)) => customerIds.map(customerId => (customerId, itemVector))
      }
      // reduciram skupljene retke za svakog custmera
      .reduceByKey(addV)
      .mapValues(utilities => utilities.toArray.sortBy(_._2).map(_._1).takeRight(N))

    recommendationMatrix.foreach(println)

  }

}
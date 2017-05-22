package hr.fer.ztel.dipl.run.add

import hr.fer.ztel.dipl.datasource.MatrixDataSource
import hr.fer.ztel.dipl.ml.CosineSimiliarityMeasure
import hr.fer.ztel.dipl.ml.SparseLinearAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object MainAddInverseJoinRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "400000000")

    val partitioner = Some(new HashPartitioner(partitions = 32))

    val measure = new CosineSimiliarityMeasure

    val itemItemMatrix = MatrixDataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure, partitioner)

    // tu mozda ne treba Set moze i Seq ako je brze ista
    val itemCustomerMatrix = MatrixDataSource.createItemCustomerMatrix("src/main/resources/transactions_10.csv", partitioner)

    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = (itemItemMatrix join itemCustomerMatrix)
      .flatMap {
        // skupljam retke za customera, nista se ne racuna
        case (_, (itemVector, customerIds)) => customerIds.map(customerId => (customerId, itemVector))
      }
      // reduciram skupljene retke za svakog custmera
      .reduceByKey(addV)
      .mapValues(utilities => utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))

    recommendationMatrix.foreach(println)

  }

}
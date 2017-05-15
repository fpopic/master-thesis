package hr.fer.ztel.dipl.run.add

import hr.fer.ztel.dipl.model.SparseVectorAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object MainAddInverseJoin {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "400000000")

    val partitioner = new HashPartitioner(1000)

    val measure = new CosineSimiliarityMeasure

    val itemItemMatrix = DataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)
      .partitionBy(partitioner)

    // tu mozda ne treba Set moze i Seq ako je brze ista
    val itemCustomerMatrix = DataSource.createItemCustomerMatrix("src/main/resources/transactions_10.csv")
      .partitionBy(partitioner)

    val partialVectors = (itemItemMatrix join itemCustomerMatrix)
      .flatMap {
        case (_, (itemVector, customerIds)) => customerIds.map(customerId => (customerId, itemVector))
      }
      .reduceByKey(add)

    val N = 5 //top n recommendations

    // reduce at workers
    val recommendationMatrix = partialVectors
      .map {
        case (customerId, utilities) => (customerId, utilities.toSeq.sortBy(_._2).map(_._1).takeRight(N))
      }

    recommendationMatrix.foreach(println)

  }

}
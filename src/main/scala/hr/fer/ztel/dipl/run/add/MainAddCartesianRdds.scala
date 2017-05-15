package hr.fer.ztel.dipl.run.add

import hr.fer.ztel.dipl.model.SparseVectorAlgebra._
import hr.fer.ztel.dipl.model._
import org.apache.spark.sql.SparkSession


object MainAddCartesianRdds {

  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val measure : ItemPairSimiliarityMeasure = new CosineSimiliarityMeasure

    val itemItemMatrix = DataSource.createItemItemMatrix("src/main/resources/item_matrix_10.csv", measure)

    val customerItemMatrix = DataSource.createCustomerItemMatrix("src/main/resources/transactions_10.csv")
      .collectAsMap.toMap

    val customerItemMatrixBroadcasted = spark.sparkContext.broadcast(customerItemMatrix)

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //todo provjeri dal crkne ako nema

    val partialVectors : Map[Int, Map[Int, Double]] =
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
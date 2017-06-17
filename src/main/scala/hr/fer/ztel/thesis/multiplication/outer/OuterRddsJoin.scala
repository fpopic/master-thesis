package hr.fer.ztel.thesis.multiplication.outer

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseVectorOperators._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object OuterRddsJoin {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark: SparkSession = handler.getSparkSession

    val measure = new CosineSimilarityMeasure(handler.normalize)
    val partitioner = Some(new HashPartitioner(16))

    val itemUserMatrix: RDD[(Int, Array[Int])] = // (item, [user])
      readItemUserMatrix(handler.userItemPath, partitioner)

    // to reduce join (shuffle size), discarding all unmatched rows/cols in item-item matrix
    val broadcastedBoughtItems = spark.sparkContext.broadcast(itemUserMatrix.keys.collect)

    val boughtItemItemMatrix: RDD[(Int, Map[Int, Double])] = // (item, [item, sim])
      readBoughtItemItemMatrix(handler.itemItemPath, measure, broadcastedBoughtItems, partitioner)

    broadcastedBoughtItems.unpersist()

    // join should produce at most 40k out of 67k tuples in shuffle-u
    val join = itemUserMatrix.join(boughtItemItemMatrix, partitioner.get)

    itemUserMatrix.unpersist()
    boughtItemItemMatrix.unpersist()

    val k = handler.topK

    val recommendations = join
      .flatMap { case (_, (userVector, itemVector)) => outer(userVector, itemVector) }
      // by (user, item) key
      .reduceByKey(_ + _)
      .map { case ((user, item), utility) => (user, (item, utility)) }
      // by (user) key
      .groupByKey
      .map { case (user, utilities) =>
        val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
        s"$user:${items.mkString(",")}"
      }

    recommendations.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendations.count} saved in: ${handler.recommendationsPath}.")

  }
}
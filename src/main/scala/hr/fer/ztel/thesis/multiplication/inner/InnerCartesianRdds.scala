package hr.fer.ztel.thesis.multiplication.inner

import hr.fer.ztel.thesis.datasource.MatrixDataSource._
import hr.fer.ztel.thesis.ml.CosineSimilarityMeasure
import hr.fer.ztel.thesis.ml.SparseVectorOperators._
import hr.fer.ztel.thesis.spark.SparkSessionHandler
import org.apache.spark.rdd.RDD

object InnerCartesianRdds {

  def main(args: Array[String]): Unit = {

    val handler = new SparkSessionHandler(args)
    implicit val spark = handler.getSparkSession

    val itemItemMatrix: RDD[(Int, Map[Int, Double])] = readItemItemMatrix(handler.itemItemPath, handler.measure)

    val userItemMatrix: RDD[(Int, Array[Int])] = readUserItemMatrix(handler.userItemPath)

    val k = handler.topK

    val recommendationMatrix = (userItemMatrix cartesian itemItemMatrix)
      .map { case ((user, userVector), (item, itemVector)) =>
        (user, (item, dot(userVector, itemVector)))
      }
      .groupByKey
      .map { case (user, utilities) =>
        val items = utilities.toArray.sortBy(_._2).map(_._1).takeRight(k)
        s"$user:${items.mkString(",")}"
      }

    recommendationMatrix.saveAsTextFile(handler.recommendationsPath)

    println(s"Recommendations: ${recommendationMatrix.count} saved in: ${handler.recommendationsPath}.")

  }

}

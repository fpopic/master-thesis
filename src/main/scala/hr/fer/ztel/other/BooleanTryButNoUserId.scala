package hr.fer.ztel.other

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Vector, Vectors}
import org.apache.spark.sql.SparkSession

object BooleanTryButNoUserId {

  def dot(a : Vector, b : Vector) : Double = {
    a.toArray.zip(b.toArray)
      .map { case (m, n) => m * n }
      .sum
  }

  def main(args : Array[String]) : Unit = {

    val warehouse = System.getProperty("user.dir") + "\\spark-warehouse"

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate

    val sc = spark.sparkContext

    val userVectors = Matrices.dense(numRows = 5, numCols = 4, Array(
      1, 0, 1, 1, 1,
      1, 0, 0, 0, 1,
      0, 0, 0, 0, 1,
      0, 1, 1, 1, 0
    ))

    val S = new IndexedRowMatrix(sc.makeRDD(Seq(
      IndexedRow(1, Vectors.dense(1.0, 0.8, 0.3, 0.5, 0.5)), // i1
      IndexedRow(2, Vectors.dense(0.8, 1.0, 0.5, 0.3, 0.7)),
      IndexedRow(3, Vectors.dense(0.3, 0.5, 1.0, 0.1, 0.8)),
      IndexedRow(4, Vectors.dense(0.5, 0.3, 0.1, 1.0, 0.2)),
      IndexedRow(5, Vectors.dense(0.5, 0.7, 0.8, 0.2, 1.0)) // i5
    )))

    val tripples = for {
      user <- userVectors.colIter
      item <- S.rows.toLocalIterator
    } yield (user, item.index, dot(user, item.vector))

    tripples.foreach(println)

  }
}

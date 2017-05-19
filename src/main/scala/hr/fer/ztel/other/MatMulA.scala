package hr.fer.ztel.other

import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.mllib.linalg.{Matrices, Vectors}
import org.apache.spark.sql.SparkSession

object MatMulA {

  def main(args : Array[String]) : Unit = {

    val warehouse = System.getProperty("user.dir") + "\\spark-warehouse"

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", warehouse)
      .getOrCreate

    val sc = spark.sparkContext

    val S = new IndexedRowMatrix(sc.makeRDD(Seq(
      IndexedRow(1, Vectors.dense(1.0, 0.8, 0.3, 0.5, 0.5)), // i1
      IndexedRow(2, Vectors.dense(0.8, 1.0, 0.5, 0.3, 0.7)),
      IndexedRow(3, Vectors.dense(0.3, 0.5, 1.0, 0.1, 0.8)),
      IndexedRow(4, Vectors.dense(0.5, 0.3, 0.1, 1.0, 0.2)),
      IndexedRow(5, Vectors.dense(0.5, 0.7, 0.8, 0.2, 1.0)) // i5
    )))

    val C = Matrices.dense(numCols = 4, numRows = 5, values = Array(
      1, 0, 1, 1, 1,
      1, 0, 0, 0, 1,
      0, 0, 0, 0, 1,
      0, 1, 1, 1, 0
    ))

    val s = Matrices.sparse(numCols = 4, numRows = 5,
      colPtrs = Array(),
      rowIndices = Array(),
      values = Array()
    )

    S.multiply(s)

    val U = S.multiply(C)

    U.rows.collect().foreach(x => println(s"i${x.index} ${x.vector}"))

  }
}

package hr.fer.ztel.zother

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartitionerRun {
  def main(args : Array[String]) : Unit = {

    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4") // za lokalno
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    val partitioner = new HashPartitioner(partitions = 4)

    val a = spark.sparkContext.makeRDD(Seq(
      (1, "A"), (2, "B"), (3, "C"), (4, "D"), (5, "E"), (6, "F"), (7, "G"), (8, "H")
    )).partitionBy(partitioner)

    val b = spark.sparkContext.makeRDD(Seq(
      (1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e"), (6, "f"), (7, "g"), (8, "h")
    )).partitionBy(partitioner)

    print("A:")
    a.foreachPartition(p => {
      p.foreach(t => print(t + " "))
      println()
    })

    print("B:")
    b.foreachPartition(p => {
      p.foreach(t => print(t + " "))
      println()
    })

    print("Join:")
    a.join(b, partitioner)
      .foreachPartition(p => {
        p.foreach(t => print(t + " "))
        println()
      })

    a.glom()

  }
}
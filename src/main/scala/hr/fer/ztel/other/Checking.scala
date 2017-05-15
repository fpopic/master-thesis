package hr.fer.ztel.other

import org.apache.spark.sql.SparkSession

object Checking {

  def main(args : Array[String]) : Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", "/media/fpopic/Data/spark-warehouse")
      .getOrCreate

    import spark.implicits._

    val f =
      Seq("a,a", "b,b", "c,c,c")
        .flatMap(str => {
          val splitted = str.split(",")
          if (splitted.size == 2)
            Some(splitted)
          else
            None
        })

    f.foreach(x => println(x(0), x(1)))

  }
}



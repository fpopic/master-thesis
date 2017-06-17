package hr.fer.ztel.thesis

import hr.fer.ztel.thesis.multiplication.block.SubMatricesMultiplication
import hr.fer.ztel.thesis.multiplication.inner.InnerCartesianRdds
import hr.fer.ztel.thesis.multiplication.outer.{OuterMapJoin, OuterMatrixEntry, OuterRddsJoin}

object Main {

  /*
    spark2-submit \
    --class hr.fer.ztel.thesis.Main \
    --master yarn --deploy-mode cluster \
    --num-executors 6 \
    --executor-cores 1 \
    --executor-memory 1G \
    --conf spark.memory.fraction=0.6 \
    --conf spark.yarn.maxAppAttempts=1 \
    /home/rovkp/fpopic/spark-recommender-assembly-1.0.jar \
    4 hdfs:///user/rovkp/fpopic/ customer_matrix.csv.indexed item_matrix.csv.indexed false recommendations 5
  */

  def main(args : Array[String]) : Unit = {

    if (args.length != 7) {
      println("Args: " + args.mkString(" "))
      println("Wrong args! [multiplication] [folder] [customer-item] [item-item] [normalize] [recommendations] [k]")
      println("[multiplication] 1=>InnerCartesianRdds, 2=>OuterMapJoin, 3=>OuterRdds, 4=>BlockMatrices, 5=>OuterMatrixEntry")
      System exit 1
    }

    args(0).toInt match {
      case 1 => InnerCartesianRdds.main(args.tail)
      case 2 => OuterMapJoin.main(args.tail)
      case 3 => OuterRddsJoin.main(args.tail)
      case 4 => SubMatricesMultiplication.main(args.tail)
      case 5 => OuterMatrixEntry.main(args.tail)
    }

  }

}

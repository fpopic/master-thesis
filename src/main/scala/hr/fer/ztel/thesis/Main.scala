package hr.fer.ztel.thesis

import hr.fer.ztel.thesis.multiplication.block.Blocks
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
    evaluation
    hdfs:///user/rovkp/fpopic/
    customer_matrix.csv.indexed
    item_matrix.csv.indexed
    llr
    false
    recommendations
    5
    1024
  */

  def main(args: Array[String]): Unit = {

    if (args.length != 9) {
      println(s"Wrong number of input args: ${args.length}")
      println("Input args: [op] [folder] [customer-item] [item-item] [measure] [normalize] [output] [k] [blocksize]")
      System exit 1
    }

    args(0) match {
      case "inner" => InnerCartesianRdds.main(args.tail)
      case "outer" => OuterRddsJoin.main(args.tail)
      case "outer-mapjoin" => OuterMapJoin.main(args.tail)
      case "blocks" => Blocks.main(args.tail)
      case "outer-entry" => OuterMatrixEntry.main(args.tail)
      case "evaluation" => EvaluationMain.main(args.tail)
    }

  }

}

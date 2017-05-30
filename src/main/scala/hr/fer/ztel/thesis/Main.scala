package hr.fer.ztel.thesis

import hr.fer.ztel.thesis.multiplication.block.BlockMatrixMultiplication
import hr.fer.ztel.thesis.multiplication.inner.InnerCartesianRdds
import hr.fer.ztel.thesis.multiplication.outer.{OuterMapJoin, OuterRdds}

object Main {

  def main(args : Array[String]) : Unit = {

    if (args.length != 6) {
      println("Wrong args, should be: [multiplication-num] [folder] [customer-item] [item-item] [recommendations] [k]")
      println("[multiplication] 1:InnerCartesianRdds, 2:OuterMapJoin, 3:OuterRdds, 4:BlockMatrixMultiplication")
      System exit 1
    }

    args(0).toInt match {
      case 1 => InnerCartesianRdds.main(args.tail)
      case 2 => OuterMapJoin.main(args.tail)
      case 3 => OuterRdds.main(args.tail)
      case 4 => BlockMatrixMultiplication.main(args.tail)
    }

  }

}

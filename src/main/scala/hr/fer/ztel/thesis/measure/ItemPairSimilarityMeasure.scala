package hr.fer.ztel.thesis.measure

trait ItemPairSimilarityMeasure extends Serializable {

  /*
   *  sim(x, x) = 0.0 default, could be overridden
   */
  val reflexiveEntryMeasure: Double = 0.0

  /**
    *
    * @param a number of customers who bought both items
    * @param b number of customers who bought only first item
    * @param c number of customers who bought only second item
    * @param d number of customers who did not buy any item
    *
    * @return similarity between a pair of items
    */
  def compute(a: Int, b: Int, c: Int, d: Int): Double

}

object ItemPairSimilarityMeasure extends Serializable {

  def parseMeasure(measure: String): Option[ItemPairSimilarityMeasure] = {
    measure match {
      case "cos" => Some(new CosineSimilarityMeasure)
      case "llr" => Some(new LogLikelihoodRatioSimilarityMeasure)
      case "yuleq" => Some(new YuleQSimilarityMeasure)
      case _ =>
        println(s""""Wrong similiarity measure: $measure! Supported measures: "cos", "llr", "yuleq"""")
        System exit 1
        None
    }
  }

}
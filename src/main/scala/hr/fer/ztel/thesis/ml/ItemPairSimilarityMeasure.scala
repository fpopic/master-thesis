package hr.fer.ztel.thesis.ml

sealed trait ItemPairSimilarityMeasure extends Serializable {

  /**
    * Normalizing the item-similarity vector to ensure that the
    * sum of all similaritiy entries for an item equals to 1.0
    */
  val normalize: Boolean = true

  /*
   *  sim(x, x) = 0.0 default, could be overridden
   */
  val reflexiveEntryMeasure: Double = 0.0

  /**
    * Depends on a measure domain, could be overridden
    */
  val missingEntryMeasure: Double = 0.0

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

  def parseMeasure(measureStr: String, normalize: Boolean): Option[ItemPairSimilarityMeasure] = {
    measureStr match {
      case "cos" => Some(new CosineSimilarityMeasure(normalize))
      case "llr" => Some(new LogLikelihoodRatioSimilarityMeasure(normalize))
      case "yuleq" => Some(new YuleQSimilarityMeasure(normalize))
      case _ =>
        println(s""""Wrong similiarity measure: $measureStr! Supported measures: "cos", "llr", "yuleq"""")
        System exit 1
        None
    }
  }

}

/**
  * Yule's Q similarity measure [-1, +1]
  */
class YuleQSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {
    val ad = a * d
    val bc = b * c
    val adbc = ad + bc
    if (adbc == 0) 0.0 else (ad - bc) / adbc.toDouble
  }
}

/**
  * Cosine similarity measure [0, 1],
  * in case of (0, 0, 0, 1) it returns 0 because Double.NaN during comparing is the biggest
  */
class CosineSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {
    if (a == 0 && b == 0 && c == 0) 0.0 // instead of NaN
    else a / math.sqrt((a + b) * (a + c))
  }
}

/**
  * Log-likelihood ratio [0, +INF]
  *
  * Source: Ted Dunning's mahout implementation:
  *
  * @see <a href="https://github.com/apache/mahout/blob/master/math/src/main/java/org/apache/mahout/math/stats
  *      /LogLikelihood.java#L62-L111">Ted Dunning's mahout implementation</a>
  */
class LogLikelihoodRatioSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {

    // x * log_e(x), nicely avoids log(0)
    def xlogx(x: Int) = if (x == 0) 0.0 else x * math.log(x)

    // shannon's entropy, not normalized
    def H(xs: Int*): Double = xlogx(xs.sum) - xs.foldLeft(0.0)(_ + xlogx(_))

    val matH = H(a, b, c, d)
    val rowH = H(a + b, c + d)
    val colH = H(a + c, b + d)

    if (rowH + colH < matH) 0.0 else 2 * (rowH + colH - matH)
  }

}
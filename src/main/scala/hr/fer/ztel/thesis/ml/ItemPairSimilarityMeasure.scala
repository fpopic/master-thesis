package hr.fer.ztel.thesis.ml

trait ItemPairSimilarityMeasure extends Serializable {

  /**
    * Normalizing the item-similarity vector to ensure that the
    * sum of all similaritiy entries for an item equals to 1.0
    */
  val normalize: Boolean = true

  /*
   *  sim(x,x) = 0.0 default, could be overridden
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

/**
  * YuleQ [-1, 1]
  */
class YuleQSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = (a * d - b * c) / (a * d + b * c)

}

/**
  * Cosine [0, 1]
  */
class CosineSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = a / math.sqrt((a + b) * (a + c))

}

/**
  * LogLikelihood [0, INF]
  */
class LogLikelihoodSimilarityMeasure(override val normalize: Boolean = true) extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {

    /**
      *
      * Shannon's Entropy
      *
      * @param x vector of size [[b]]
      *
      * @return entropy measure [0, 1]
      */
    def H(x: Int*): Double = {
      import math.log10

      val base = x.length

      def logB(x_ : Double) = log10(x_) / log10(base)

      val xs = x.product

      xs * logB(xs) - x.map(x => x * logB(x)).sum
    }

    2 * (a + b + c + d) * (H(a, b, c, d) - H(a + b, c + d) - H(a + c, b + d))
  }

}
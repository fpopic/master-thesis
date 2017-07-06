package hr.fer.ztel.thesis.measure

/**
  * Log-likelihood ratio [0, +INF]
  *
  * Source: Ted Dunning's mahout implementation
  *
  * @see <a href="https://github.com/apache/mahout/blob/master/math/src/main/java/org/apache/mahout/math/stats
  *      /LogLikelihood.java#L62-L111">Ted Dunning's mahout implementation</a>
  */
class LogLikelihoodRatioSimilarityMeasure extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {

    // x * log_e(x), nicely avoids log(0)
    def xlogx(x: Int) = if (x == 0) 0.0 else x * math.log(x)

    // Shannon's entropy, not normalized
    def H(xs: Int*) = xlogx(xs.sum) - xs.foldLeft(0.0)(_ + xlogx(_))

    val matH = H(a, b, c, d)
    val rowH = H(a + b, c + d)
    val colH = H(a + c, b + d)

    if (rowH + colH < matH) 0.0 else 2 * (rowH + colH - matH)
  }

}
package hr.fer.ztel.thesis.measure

/**
  * Yule's Q similarity measure [-1, +1]
  */
class YuleQSimilarityMeasure extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {
    val ad = a * d
    val bc = b * c
    val adbc = ad + bc
    if (adbc == 0) 0.0 else (ad - bc) / adbc.toDouble
  }
}
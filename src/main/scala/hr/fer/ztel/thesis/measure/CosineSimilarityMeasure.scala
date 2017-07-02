package hr.fer.ztel.thesis.measure

/**
  * Cosine similarity measure [0, 1],
  * in case of (0, 0, 0, 1) it returns 0 because Double.NaN during comparing is the biggest
  */
class CosineSimilarityMeasure extends ItemPairSimilarityMeasure {

  def compute(a: Int, b: Int, c: Int, d: Int): Double = {
    if (a == 0 && b == 0 && c == 0) 0.5 // instead of NaN todo check it
    else a / math.sqrt((a + b) * (a + c))
  }

}

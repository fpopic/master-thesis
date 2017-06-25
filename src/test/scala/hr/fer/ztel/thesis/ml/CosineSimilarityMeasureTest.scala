package hr.fer.ztel.thesis.ml

import org.scalatest.{FlatSpec, Matchers}

class CosineSimilarityMeasureTest extends FlatSpec with Matchers {

  /**
    * f(a, b, c, d) = a / math.sqrt((a + b) * (a + c))
    */
  val measure: ItemPairSimilarityMeasure = new CosineSimilarityMeasure

  "Cosine " should " return min value." in {

    val (a, b, c, d) = (0, 1, 1, 0)

    val actual = measure.compute(a, b, c, d)
    val expected = 0.0

    actual shouldEqual expected
  }

  "Cosine " should " return middle value." in {

    val (a, b, c, d) = (1, 1, 1, 1)

    val actual = measure.compute(a, b, c, d)
    val expected = 0.5

    actual shouldEqual expected
  }

  "Cosine " should " return max value." in {

    val (a, b, c, d) = (1, 0, 0, 1)

    val actual = measure.compute(a, b, c, d)
    val expected = 1

    actual shouldEqual expected
  }

  "Cosine " should " return middle value in case of dividing by zero" in {

    val (a, b, c, d) = (0, 0, 0, 0)

    val actual = measure.compute(a, b, c, d)
    val expected = 0.5

    actual shouldEqual expected
  }

}
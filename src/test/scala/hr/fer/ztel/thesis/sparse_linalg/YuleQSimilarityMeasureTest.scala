package hr.fer.ztel.thesis.sparse_linalg

import hr.fer.ztel.thesis.measure.{ItemPairSimilarityMeasure, YuleQSimilarityMeasure}
import org.scalatest.{FlatSpec, Matchers}

class YuleQSimilarityMeasureTest extends FlatSpec with Matchers {

  /**
    * f(a, b, c, d) = (a * d - b * c) / (a * d + b * c) in [-1 , +1]
    */
  val measure: ItemPairSimilarityMeasure = new YuleQSimilarityMeasure

  "Yule's Q " should " return min value." in {

    val (a, b, c, d) = (0, 1, 1, 0)

    val actual = measure.compute(a, b, c, d)
    val expected = -1.0

    actual shouldEqual expected
  }

  "Yule's Q " should " return middle value." in {

    val (a, b, c, d) = (1, 1, 1, 1)

    val actual = measure.compute(a, b, c, d)
    val expected = 0.0

    actual shouldEqual expected
  }

  "Yule's Q " should " return max value." in {

    val (a, b, c, d) = (1, 0, 0, 1)

    val actual = measure.compute(a, b, c, d)
    val expected = +1.0

    actual shouldEqual expected
  }

  "Yule's Q " should " return middle value in case of dividing by zero" in {

    val (a, b, c, d) = (0, 0, 0, 0)

    val actual = measure.compute(a, b, c, d)
    val expected = 0.0

    actual shouldEqual expected
  }

}
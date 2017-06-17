package org.apache.spark.mllib.linalg

import breeze.linalg.{Vector => BreezeVector}
import org.apache.spark.mllib.linalg.{Vector => MLlibVector}

object MLlibBreezeConversions {

  implicit class MLlibVectorPublications(val vector : MLlibVector) extends AnyVal {
    def toBreeze : BreezeVector[Double] = vector.asBreeze
  }

  implicit class BreezeVectorPublications(val breezeVector : BreezeVector[Double]) extends AnyVal {
    def toMLlib : MLlibVector = Vectors.fromBreeze(breezeVector)
  }

}
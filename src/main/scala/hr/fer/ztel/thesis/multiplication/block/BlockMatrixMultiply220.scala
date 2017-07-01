/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.mllib.linalg._
import org.apache.spark.{Partitioner, SparkException}

object BlockMatrixMultiply220 extends Serializable {

  /** Block (i,j) --> Set of destination partitions */
  private type BlockDestinations = Map[(Int, Int), Set[Int]]

  def multiply(first: BlockMatrix, second: BlockMatrix, numMidDimSplits: Int = 1): BlockMatrix = {
    require(first.numCols() == second.numRows(), "The number of columns of A and the number of rows " +
      s"of B must be equal. A.numCols: ${first.numCols()}, B.numRows: ${second.numRows()}. If you " +
      "think they should be equal, try setting the dimensions of A and B explicitly while " +
      "initializing them.")
    require(numMidDimSplits > 0, "numMidDimSplits should be a positive integer.")

    def simulateMultiply(
      partitioner: GridPartitioner,
      midDimSplitNum: Int): (BlockDestinations, BlockDestinations) = {

      lazy val firstBlockInfo = first.blocks.mapValues(block => (block.numRows, block.numCols)).cache()
      lazy val otherBlockInfo = second.blocks.mapValues(block => (block.numRows, block.numCols)).cache()

      val leftMatrix = firstBlockInfo.keys.collect()
      val rightMatrix = otherBlockInfo.keys.collect()

      val rightCounterpartsHelper = rightMatrix.groupBy(_._1).mapValues(_.map(_._2))
      val leftDestinations = leftMatrix.map { case (rowIndex, colIndex) =>
        val rightCounterparts = rightCounterpartsHelper.getOrElse(colIndex, Array.empty[Int])
        val partitions = rightCounterparts.map(b => partitioner.getPartition((rowIndex, b)))
        val midDimSplitIndex = colIndex % midDimSplitNum
        ((rowIndex, colIndex),
          partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
      }.toMap

      val leftCounterpartsHelper = leftMatrix.groupBy(_._2).mapValues(_.map(_._1))
      val rightDestinations = rightMatrix.map { case (rowIndex, colIndex) =>
        val leftCounterparts = leftCounterpartsHelper.getOrElse(rowIndex, Array.empty[Int])
        val partitions = leftCounterparts.map(b => partitioner.getPartition((b, colIndex)))
        val midDimSplitIndex = rowIndex % midDimSplitNum
        ((rowIndex, colIndex),
          partitions.toSet.map((pid: Int) => pid * midDimSplitNum + midDimSplitIndex))
      }.toMap

      (leftDestinations, rightDestinations)
    }

    if (first.colsPerBlock == second.rowsPerBlock) {
      val resultPartitioner = GridPartitioner(first.numRowBlocks, second.numColBlocks,
        math.max(first.blocks.partitions.length, second.blocks.partitions.length))
      val (leftDestinations, rightDestinations) = simulateMultiply(resultPartitioner, numMidDimSplits)
      // Each block of A must be multiplied with the corresponding blocks in the columns of B.
      val flatA = first.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
        val destinations = leftDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
      }
      // Each block of B must be multiplied with the corresponding blocks in each row of A.
      val flatB = second.blocks.flatMap { case ((blockRowIndex, blockColIndex), block) =>
        val destinations = rightDestinations.getOrElse((blockRowIndex, blockColIndex), Set.empty)
        destinations.map(j => (j, (blockRowIndex, blockColIndex, block)))
      }
      val intermediatePartitioner = new Partitioner {
        override def numPartitions: Int = resultPartitioner.numPartitions * numMidDimSplits

        override def getPartition(key: Any): Int = key.asInstanceOf[Int]
      }
      val newBlocks = flatA.cogroup(flatB, intermediatePartitioner).
        flatMap { case (pId, (a, b)) =>
          a.flatMap { case (leftRowIndex, leftColIndex, leftBlock) =>
            b.filter(_._1 == leftColIndex).map { case (rightRowIndex, rightColIndex, rightBlock) =>
              val C = rightBlock match {
                case dense: DenseMatrix => leftBlock.multiply(dense)
                case sparse: SparseMatrix => leftBlock.multiply(sparse.toDense)
                case _ =>
                  throw new SparkException(s"Unrecognized matrix type ${rightBlock.getClass}.")
              }
              ((leftRowIndex, rightColIndex), C.asBreeze)
            }
          }
        }.reduceByKey(resultPartitioner, (a, b) => a + b).mapValues(Matrices.fromBreeze)
      new BlockMatrix(newBlocks, first.rowsPerBlock, second.colsPerBlock, first.numRows(), second.numCols())
    } else {
      throw new SparkException("colsPerBlock of A doesn't match rowsPerBlock of B. " +
        s"A.colsPerBlock: ${first.colsPerBlock}, B.rowsPerBlock: ${second.rowsPerBlock}")
    }
  }
}
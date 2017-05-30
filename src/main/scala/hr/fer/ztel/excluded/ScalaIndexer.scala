package hr.fer.ztel.excluded

import java.io.FileWriter

import hr.fer.ztel.thesis.datasource.DataSourceModelValidator

import scala.io.Source

object ScalaIndexer extends Serializable {
  def transformItemIdsToIndexes(inputPath : String, outputPath : String, lookupPath : String) : Unit = {

    var lineCounter = 0
    var counter = 0
    val lookup = collection.mutable.Map.empty[Int, Int] // id -> index

    val inputFile = Source.fromFile(inputPath)
    val outputFile = new FileWriter(outputPath, true)
    val lookupFile = new FileWriter(lookupPath, true)

    val endl = System.lineSeparator

    val start = System.nanoTime

    inputFile.getLines.foreach {
      line => {
        val t = line.split(",")
        if (DataSourceModelValidator.isParsableItemItemRecord(t)) {
          val itemId1 = t(0).toInt
          val itemId2 = t(1).toInt
          val itemIndex1 = lookup.getOrElseUpdate(itemId1, {counter = counter + 1; counter})
          val ItemIndex2 = lookup.getOrElseUpdate(itemId2, {counter = counter + 1; counter})
          val outline = (itemIndex1, ItemIndex2, t(2), t(3), t(4), t(5)).productIterator.mkString(",")
          outputFile.write(s"$outline$endl")
        }
        else {
          // header
          outputFile.write(s"$line$endl")
        }
        outputFile.flush()
        lineCounter = lineCounter + 1
        if (lineCounter % 1e6 == 0) println(s"Writing line:$lineCounter")
      }
    }

    inputFile.close()
    outputFile.close()

    println(s"Time:${(System.nanoTime() - start) / 1e9}s.")

    lookup.foreach {
      case (itemId, itemIndex) =>
        lookupFile.write(s"$itemId,$itemIndex$endl")
        lookupFile.flush()
    }

    lookupFile.close()
  }

}

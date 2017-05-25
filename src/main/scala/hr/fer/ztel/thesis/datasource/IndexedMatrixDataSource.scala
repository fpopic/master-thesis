package hr.fer.ztel.thesis.datasource

import hr.fer.ztel.thesis.datasource.DataSourceModelValidator.{isParsableCustomerItemRecord, isParsableItemItemRecord}
import hr.fer.ztel.thesis.ml.ItemPairSimilarityMeasure
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object IndexedMatrixDataSource extends Serializable {

  def persistItemIdIndexLookup(inputPath : String, lookupPath : String)
    (implicit spark : SparkSession) : Dataset[(Int, Int)] = {

    import hr.fer.ztel.thesis.spark.DataFrameExtensions.implicits._
    import spark.implicits._

    val start = System.nanoTime

    val ds = spark.read
      .textFile(inputPath)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .map(r => (r(0).toInt, r(1).toInt))
      .toDF("itemId1", "itemId2")
      .cache

    val lookup = (ds.select('itemId1).distinct union ds.select('itemId2).distinct).distinct.toDF("itemId")
      .myZipWithIndex("itemIndex")

    lookup.write.csv(lookupPath)

    println(s"Time:${(System.nanoTime() - start) / 1e9}s.")

    lookup.as[(Int, Int)]
  }

  def persistCustomerIdIndexLookup(inputPath : String, lookupPath : String)
    (implicit spark : SparkSession) : Dataset[(Int, Int)] = {

    import hr.fer.ztel.thesis.spark.DataFrameExtensions.implicits._
    import spark.implicits._

    val start = System.nanoTime

    val lookup = spark
      .read
      .textFile(inputPath)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map(_ (0).toInt)
      .toDF("customerId")
      .distinct()
      .myZipWithIndex("customerIndex")

    lookup.write.csv(lookupPath)

    println(s"Time:${(System.nanoTime() - start) / 1e9}s.")

    lookup.as[(Int, Int)]
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def createItemItemMatrix(path : String, measure : ItemPairSimilarityMeasure,
    itemsLookup : Dataset[(Int, Int)], partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import spark.implicits._

    val broadcastedLookup = spark.sparkContext.broadcast(itemsLookup.collect)

    val itemItemRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableItemItemRecord(_))
      .mapPartitions(partition => {
        val localLookup = broadcastedLookup.value.toMap
        partition.map(t => {
          val itemIndex1 = localLookup(t(0).toInt)
          val itemIndex2 = localLookup(t(1).toInt)
          (itemIndex1, itemIndex2, t(2).toInt, t(3).toInt, t(4).toInt, t(5).toInt)
        })
      })
      .flatMap {
        case (item1, item2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // associative item-item entries (x, y) (y, x)
          Seq((item1, (item2, similarity)), (item2, (item1, similarity)))
      }
      .rdd

    val itemVectors =
      if (partitioner.isDefined)
        itemItemRDD.groupByKey(partitioner.get)
      else
        itemItemRDD.groupByKey

    if (measure.normalize)
      itemVectors.mapValues(itemVector => {
        val norm = itemVector.foldLeft(0.0)((sum, t) => sum + t._2)
        if (norm != 0.0)
          itemVector.map { case (item, similarity) => (item, similarity / norm) }.toMap
        else
          itemVector.toMap
      })

    else
      itemVectors.mapValues(itemVector => itemVector.toMap)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def createItemCustomerMatrix(path : String, itemsLookup : Dataset[(Int, Int)],
    customersLookup : Dataset[(Int, Int)], partitioner : Option[Partitioner] = None)
    (implicit spark : SparkSession) : RDD[(Int, Array[Int])] = {

    import spark.implicits._

    val broadcastedItemsLookup = spark.sparkContext.broadcast(itemsLookup.collect)
    val broadcastedCustomersLookup = spark.sparkContext.broadcast(customersLookup.collect)

    val itemCustomerRDD = spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .mapPartitions(partition => {
        val localItemsLookup = broadcastedItemsLookup.value.toMap
        val localCustomersLookup = broadcastedCustomersLookup.value.toMap
        partition.map {
          case Array(customerId, _, itemId, quantity) =>
            val customerIndex = localCustomersLookup(customerId.toInt)
            val itemIndex = localItemsLookup(itemId.toInt)
            (itemIndex, customerIndex, quantity.toDouble)
        }
      })
      .toDF("item", "customer", "quantity")
      .groupBy("item", "customer")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd

    if (partitioner.isDefined)
      itemCustomerRDD
        .groupByKey(partitioner = partitioner.get)
        .mapValues(customerVector => customerVector.toArray)

    else
      itemCustomerRDD
        .groupByKey
        .mapValues(customerVector => customerVector.toArray)
  }

  def createCustomerItemMatrix(path : String, itemsLookup : Dataset[(Int, Int)],
    customersLookup : Dataset[(Int, Int)])
    (implicit spark : SparkSession) : RDD[(Int, Array[Int])] = {

    import spark.implicits._

    val broadcastedItemsLookup = spark.sparkContext.broadcast(itemsLookup.collect)
    val broadcastedCustomersLookup = spark.sparkContext.broadcast(customersLookup.collect)

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .mapPartitions(partition => {
        val localItemsLookup = broadcastedItemsLookup.value.toMap
        val localCustomersLookup = broadcastedCustomersLookup.value.toMap
        partition.map {
          case Array(customerId, _, itemId, quantity) =>
            val customerIndex = localCustomersLookup(customerId.toInt)
            val itemIndex = localItemsLookup(itemId.toInt)
            (customerIndex, itemIndex, quantity.toDouble)
        }
      })
      .toDF("customer", "item", "quantity")
      .groupBy("customer", "item")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd
      .groupByKey
      .mapValues(itemVector => itemVector.toArray)
  }


}
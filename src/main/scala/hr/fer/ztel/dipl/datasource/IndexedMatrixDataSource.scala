package hr.fer.ztel.dipl.datasource

import hr.fer.ztel.dipl.ml.ItemPairSimiliarityMeasure
import hr.fer.ztel.dipl.model._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object IndexedMatrixDataSource extends Serializable {


  private def createAndPersistItemIdIndexLookup(inputPath : String, lookupPath : String)
    (implicit spark : SparkSession) : Dataset[(Int, Int)] = {

    import DataFrameExtensions.implicits._
    import ItemItemRecord.isParsableItemItemRecord
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

  private def createAndPersistCustomerIdIndexLookup(inputPath : String, lookupPath : String)
    (implicit spark : SparkSession) : Dataset[(Int, Int)] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
    import DataFrameExtensions.implicits._
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

  def createItemItemMatrix(path : String, measure : ItemPairSimiliarityMeasure, lookup : Dataset[(Int, Int)],
    partitioner : Option[Partitioner] = None)(implicit spark : SparkSession) : RDD[(Int, Map[Int, Double])] = {

    import ItemItemRecord.isParsableItemItemRecord
    import spark.implicits._

    val broadcastedLookup = spark.sparkContext.broadcast(lookup.collect)

    val customerItemRDD = spark.read
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
        case (itemIndex1, itemIndex2, a, b, c, d) =>
          val similarity = measure.compute(a, b, c, d)
          // associative item-item entries (x, y) (y, x)
          Seq((itemIndex1, (itemIndex2, similarity)), (itemIndex2, (itemIndex1, similarity)))
      }
      .rdd

    if (partitioner.isDefined)
      customerItemRDD
        .groupByKey(partitioner.get)
        .mapValues(itemVector => itemVector.toMap)

    else
      customerItemRDD
        .groupByKey
        .mapValues(itemVector => itemVector.toMap)
  }

  def createItemCustomerMatrix(path : String, itemsLookup : Dataset[(Int, Int)], customersLookup : Dataset[(Int, Int)],
    partitioner : Option[Partitioner] = None)(implicit spark : SparkSession) : RDD[(Int, Set[Int])] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
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
          case Array(customerId, _, itemId, quantity) => {
            val customerIndex = localCustomersLookup(customerId.toInt)
            val itemIndex = localItemsLookup(itemId.toInt)
            (itemIndex, customerIndex, quantity.toDouble)
          }
        }
      })
      .toDF("itemIndex", "customerIndex", "quantity")
      .groupBy("customerIndex", "itemIndex")
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[(Int, Int)]
      .rdd

    if (partitioner.isDefined)
      itemCustomerRDD
        .groupByKey(partitioner = partitioner.get)
        .mapValues(customerVector => customerVector.toSet)

    else
      itemCustomerRDD
        .groupByKey
        .mapValues(customerVector => customerVector.toSet)
  }

  def createCustomerItemMatrix(path : String)
    (implicit spark : SparkSession) : RDD[(Int, Set[Int])] = {

    import CustomerItemRecord.isParsableCustomerItemRecord
    import spark.implicits._

    spark.read
      .textFile(path)
      .map(_.split(","))
      .filter(isParsableCustomerItemRecord(_))
      .map {
        case Array(customerId, _, itemId, quantity) => CustomerItemRecord(customerId.toInt, itemId.toInt, quantity.toDouble)
      }
      .groupBy('customerId, 'itemId)
      .agg("quantity" -> "sum")
      .where($"sum(quantity)" >= 1.0)
      .drop($"sum(quantity)")
      .as[CustomerItem]
      .map {
        case CustomerItem(customerId, itemId) => (customerId, itemId)
      }
      .rdd
      .groupByKey
      .mapValues(itemVector => itemVector.toSet)
  }


  def transformItemMatrixIds() : Unit = {

  }

  def transformCustomerMatrixIds() : Unit = {

  }


  def main(args : Array[String]) : Unit = {

    val warehouse = "/media/fpopic/Data/spark-warehouse/"

    val itemItemInputPath = warehouse + "item_matrix.csv"
    val itemItemOutputPath = warehouse + "item_matrix_indexed.csv"
    val ItemsLookupPath = warehouse + "item_matrix_lookup.csv"

    val customerItemInputPath = warehouse + "customer_matrix.csv"
    val customerItemOutputPath = warehouse + "customer_matrix_indexed.csv"
    val customersLookupPath = warehouse + "customer_matrix_lookup.csv"


    implicit val spark = SparkSession.builder
      .master("local[*]")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.warehouse.dir", warehouse)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "hr.fer.ztel.datasource.SparseKryoRegistrator")
      .getOrCreate

    createAndPersistItemIdIndexLookup(itemItemInputPath, ItemsLookupPath)

    //    todo transformiraj obje kolone s jednim lookupom

    //        createAndPersistItemIdIndexLookup(customerItemInputPath, customersLookupPath)

    //todo transformiraj obje kolone s oba lookupa

  }


}
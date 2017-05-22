package hr.fer.ztel.dipl.datasource

object DataFrameExtensions {

  object implicits {

    import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
    import org.apache.spark.sql.{DataFrame, Row}

    implicit class DataFrameExtensions(val df : DataFrame) extends AnyVal {

      /**
        *
        * @param colName column name of new index column
        * @param offset  start indexing from offset
        * @param inFront position of the new index column in dataframe (front, back)
        *
        * @return
        */
      def myZipWithIndex(colName : String, offset : Int = 0, inFront : Boolean = false) : DataFrame = {
        if (inFront)
          df.sparkSession.createDataFrame(
            rowRDD = df.rdd.zipWithIndex.map {
              case (row, index) => Row.fromSeq(Seq(index.toInt + offset) ++ row.toSeq ++ Seq())
            },
            schema = StructType {
              Array(StructField(colName, IntegerType, nullable = false)) ++ df.schema.fields ++ Array[StructField]()
            }
          )
        else
          df.sparkSession.createDataFrame(
            rowRDD = df.rdd.zipWithIndex.map {
              case (row, index) => Row.fromSeq(Seq() ++ row.toSeq ++ Seq(index.toInt + offset))
            },
            schema = StructType {
              Array[StructField]() ++ df.schema.fields ++ Array(StructField(colName, IntegerType, nullable = false))
            }
          )
      }
    }

  }


}

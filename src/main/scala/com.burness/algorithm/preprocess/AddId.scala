package com.burness.algorithm.preprocess

import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import scopt.OptionParser

/**
 * Created by burness on 16-6-24.
 */
class AddId(spark: SparkSession) {
  case class Params(inputTableName: String = null,
                    outputTableName: String = null,
                    addIdName: String = null
                   )
    extends AbstractParams[Params]

  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Join two table") {
      head("add a Id column with the input table")
      opt[String]("inputTableName")
        .text("input table name")
        .action((x, c) => c.copy(inputTableName = x))
      opt[String]("outputTableName")
        .text("output table name")
        .action((x,c) => c.copy(outputTableName = x))
      opt[String]("addIdName")
        .text("add Id Name")
        .action((x,c) => c.copy(addIdName = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
          |
        """.stripMargin
      )
    }
    parser.parse(args, defaultParams) match{
      case Some(params) =>
        params
      case None =>
        defaultParams
    }
  }

  def run(params: Params): Unit ={
    val inputDF = spark.sql(s"select * from ${params.inputTableName}")
    val inputRddIndex = inputDF.rdd.zipWithIndex().map{
      case (k,v) =>
        val tempSeq = v +: k.toSeq
        Row.fromSeq(tempSeq)
    }

    val newField = StructField(params.addIdName, LongType, false)
    val newStructFields = newField +: inputDF.schema.fields
    val result = spark.createDataFrame(inputRddIndex, StructType(newStructFields))
    result.write.saveAsTable(params.outputTableName)
  }
}

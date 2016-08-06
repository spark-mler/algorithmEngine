package com.burness.algorithm.preprocess

import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

/**
 * Created by burness on 16-6-27.
 */
class Standardization (sc: SparkContext){
  case class Params(inputTableName: String = null,
  outputTableName: String = null,
  //col1,col2,col3
  standardColumnsName: String = null,
  isAddStandardColumns: Boolean = false
  )
  extends AbstractParams[Params]

  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("normalize table columns") {
      head("input table name")
      opt[String]("inputTableName")
        .text("input table name")
        .action((x, c) => c.copy(inputTableName = x))
      opt[String]("outputTableName")
        .text("output table name")
        .action((x,c) => c.copy(outputTableName = x))
      opt[String]("standardColumnsName")
        .text("standard columns name")
        .action((x,c) => c.copy(standardColumnsName = x))
      opt[Boolean]("isAddStandardColumns")
        .text("whether to add standard columns")
        .action((x,c) => c.copy(isAddStandardColumns = x))
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
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
    import org.apache.spark.sql.functions._
    var inputDF = sql(s"select * from ${params.inputTableName}")
    if (!params.isAddStandardColumns) {
      params.standardColumnsName.split(",").map {
        case s =>
          val (meanVal, stddevVal) = inputDF.agg(mean(s), stddev(s)).first match {
            case Row(x:Double, y: Double) => (x,y)
          }
          val standardDF = (inputDF(s)- meanVal)/stddevVal
          inputDF = inputDF.withColumn(s, standardDF)
      }
      inputDF.write.saveAsTable(params.outputTableName)

    }else{
      params.standardColumnsName.split(",").map {
        case s =>
          val (meanVal, stddevVal) = inputDF.agg(mean(s), stddev(s)).first match {
            case Row(x:Double, y: Double) => (x,y)
          }
          val standardDF = (inputDF(s)- meanVal)/stddevVal
          inputDF = inputDF.withColumn("standard_"+s, standardDF)
      }
      inputDF.write.saveAsTable(params.outputTableName)

    }
  }
}

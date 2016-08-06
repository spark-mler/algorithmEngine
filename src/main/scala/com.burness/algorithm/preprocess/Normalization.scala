package com.burness.algorithm.preprocess

import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.sql.Row
import org.apache.spark.sql.ColumnName
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

/**
 * Created by burness on 16-6-24.
 */
class Normalization(sc: SparkContext) {
  case class Params(inputTableName: String = null,
                    outputTableName: String = null,
                   //col1,min1,max1\tcol2,min2,max2\tcol3,min3,max3
                    normalizeColumnsName: String = null,
                    isAddNormalizeColumns: Boolean = false
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
      opt[String]("normalizeColumnsName")
        .text("normalize columns name")
        .action((x,c) => c.copy(normalizeColumnsName = x))
      opt[Boolean]("isAddNormalizeColumns")
        .text("whether to add normalize columns")
        .action((x,c) => c.copy(isAddNormalizeColumns = x))
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
    var inputDF = sql(s"select * from ${params.inputTableName}")
    if(!params.isAddNormalizeColumns){
    params.normalizeColumnsName.split("\t").map{
      case s =>
        val sList = s.split(",")
        val tempCol = sList(0)
        val tempMin = lit(sList(1).toDouble)
        val tempMax = lit(sList(2).toDouble)


        val (colMin, colMax) = inputDF.agg(min(tempCol),max(tempCol)).first match {
          case  Row(x: Double, y: Double) => (x, y)
        }

        val tempNormalized = (inputDF(tempCol) - colMin) / (colMax - colMin)
        val tempScaled = (tempMax-tempMin) * tempNormalized + tempMin
        inputDF = inputDF.withColumn(tempCol, tempScaled)

    }
    inputDF.write.saveAsTable(params.outputTableName)

  }else{
      params.normalizeColumnsName.split("\t").map {
        case s =>
          val sList = s.split(",")
          val tempCol = sList(0)
          val tempMin = lit(sList(1).toDouble)
          val tempMax = lit(sList(2).toDouble)


          val (colMin, colMax) = inputDF.agg(min(tempCol), max(tempCol)).first match {
            case Row(x: Double, y: Double) => (x, y)
          }

          val tempNormalized = (inputDF(tempCol) - colMin) / (colMax - colMin)
          val tempScaled = (tempMax - tempMin) * tempNormalized + tempMin
          inputDF = inputDF.withColumn("normal_"+tempCol, tempScaled)

      }
      inputDF.write.saveAsTable(params.outputTableName)}
  }

}

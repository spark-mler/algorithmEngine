package com.burness.algorithm.preprocess

import breeze.numerics._
import com.burness.utils.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import scopt.OptionParser

/**
 * Created by burness on 16-6-22.
 */
class ColumnUnion(sc: SparkContext) {

  case class Params(leftTableName: String = null,
                    rightTableName: String = null,
                    outputTableName: String = null,
                    leftOutFieldsName: String = null,
                    rightOutFieldsName: String = null
  )
    extends AbstractParams[Params]

  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("Join two table") {
      head("Join two table Params parse")
      opt[String]("leftTableName")
        .text("left table name")
        .action((x, c) => c.copy(leftTableName = x))
      opt[String]("rightTableName")
        .text("right table name")
        .action((x,c) => c.copy(rightTableName = x))
      opt[String]("outputTableName")
        .text("output table name")
        .action((x,c) => c.copy(outputTableName = x))
      opt[String]("leftOutFieldsName")
        .text("left out fields name")
        .action((x,c) => c.copy(leftOutFieldsName = x))
      opt[String]("rightOutFieldsName")
        .text("right out fields name")
        .action((x,c) => c.copy(rightOutFieldsName = x))
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
    Logger.getRootLogger.setLevel(Level.WARN)
    val hiveContext = new HiveContext(sc)

    import hiveContext.implicits._
    import hiveContext.sql
    var leftDF = sql(s"select *  from ${params.leftTableName}")
    val outputLeftRenameFields = params.leftOutFieldsName.split("\t").map{
      case s =>
        val sList = s.split(",")
        if(sList.length >= 2) {
          leftDF = leftDF.withColumnRenamed(sList(0),sList(1))
          sList(1)
        }else{
          s
        }
    }.toSeq
    var rightDF = sql(s"select * from ${params.rightTableName}")
    val outputRightRenameFields = params.rightOutFieldsName.split("\t").map{
      case s =>
        val sList = s.split(",")
        if(sList.length >= 2) {
          rightDF = rightDF.withColumnRenamed(sList(0),sList(1))
          sList(1)
        }else{
          s
        }
    }.toSeq
    val leftCount = leftDF.count()
    val rightCount = rightDF.count()
    assert(leftCount == rightCount, "Left table should be the same rows with the right table")
    val outputFields = outputLeftRenameFields++outputRightRenameFields

    val leftRdd = leftDF.rdd
    val rightRdd = rightDF.rdd

    val unionRdd = leftRdd.zip(rightRdd).map{
      case (k,v) =>
        val add = k.toSeq ++ v.toSeq
        Row.fromSeq(add)
    }

    val unionDF = hiveContext.createDataFrame(unionRdd, StructType(leftDF.schema.fields ++ rightDF.schema.fields))
    val result = unionDF.select(outputFields.head, outputFields.tail:_*)
    result.write.saveAsTable(params.outputTableName)
  }


}

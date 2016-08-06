package com.burness.algorithm.preprocess

import com.burness.utils.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import scopt.OptionParser

/**
 * Created by burness on 16-6-23.
 */
class Union(sc: SparkContext) {
  case class Params(leftTableName: String = null,
                    rightTableName: String = null,
                    outputTableName: String = null,
                    removeDeplicate: Boolean = false
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
      opt[Boolean]("removeDeplicate")
        .text("remove the Deplicate rows")
        .action((x,c)=>c.copy(removeDeplicate = x))
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
    val leftDF = sql(s"select *  from ${params.leftTableName}")
    val rightDF = sql(s"select * from ${params.rightTableName}")

    var result = leftDF.unionAll(rightDF)
    if (params.removeDeplicate){
      result = leftDF.distinct.unionAll(rightDF.distinct).distinct
    }

    result.write.saveAsTable(params.outputTableName)
  }
}

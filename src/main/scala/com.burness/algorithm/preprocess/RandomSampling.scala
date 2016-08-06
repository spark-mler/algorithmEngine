package com.burness.algorithm.preprocess

import breeze.numerics.abs
import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

/**
 * Created by burness on 16-6-20.
 */
class RandomSampling(sc: SparkContext) {



  case class Params(samplingRatio: Double =1.0,
                   inputTableName: String = null,
                    outputTableName: String = null)
  extends AbstractParams[Params]


  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("RandomSampling") {
      head("Random Sampling Params parse")
      opt[String]("inputTableName")
        .text("data input path")
        .action((x, c) => c.copy(inputTableName = x))
      opt[String]("outputTableName")
        .text("data output path")
        .action((x, c) => c.copy(outputTableName = x))
      opt[Double]("samplingRatio")
        .text("random sampling ratio")
        .action((x, c) => c.copy(samplingRatio = x))
    }

      parser.parse(args, defaultParams) match {
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
    // 向数据表中增加一列，比如0.7，那么增加一列随机值在0-9的，小于等于6的即可
    val result = sql(s"select * from ${params.inputTableName}").sample(withReplacement = false, params.samplingRatio)
    val r = scala.util.Random
    r.setSeed(System.currentTimeMillis())
    val tempNum = abs(r.nextInt())
    val tempName = "random_"+tempNum.toString+"_sample_table"
    result.registerTempTable(tempName)
    sql(s"create table ${params.outputTableName} as select * from $tempName")

  }


}

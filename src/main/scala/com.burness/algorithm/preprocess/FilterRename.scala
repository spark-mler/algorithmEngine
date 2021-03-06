package com.burness.algorithm.preprocess

import breeze.numerics.abs
import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import scopt.OptionParser

/**
 * Created by burness on 16-6-20.
 */
class FilterRename(sc: SparkContext) {
  case class Params(filterFields: String = null,
                    filterConditions: String = null,
                    renameFields: String = null,
                    inputTableName: String = null,
                    outputTableName: String = null)
    extends AbstractParams[Params]


  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("FilterRename") {
      head("FilterRename Params parse")
      opt[String]("inputTableName")
        .text("data input path")
        .action((x, c) => c.copy(inputTableName = x))
      opt[String]("outputTableName")
        .text("data output path")
        .action((x, c) => c.copy(outputTableName = x))
      // field1,field2,field3,field4
      opt[String]("filterFields")
        .text("filter fields")
        .action((x,c)=>c.copy(filterFields=x))
      opt[String]("filterConditions")
        .text("filter fields Conditions")
        .action((x, c) => c.copy(filterConditions = x))
      // field1=> fieldA, field2=> fieldB
      // 字符串格式如下：field1,fieldA\tfield2,fieldB
      opt[String]("renameFields")
        .text("rename fields")
        .action((x,c) => c.copy(renameFields = x))

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
    val result = sql(s"select ${params.filterFields} from ${params.inputTableName} where ${params.filterConditions}")
    // rename字段
    val r = scala.util.Random
    r.setSeed(System.currentTimeMillis())
    val tempNum = abs(r.nextInt())
    val tempName = "random_"+tempNum.toString+"_sample_table"
    result.registerTempTable(tempName)
    // rename字段名必须是fieldNames的子集
    val renameFields = params.renameFields.split("\t").map{
      case s =>
        val originField = s.split(",")(0)
        val namedField = s.split(",")(1)
        (originField, namedField)
    }
    val renameSet = renameFields.map{
      case (k,v) =>
        k
    }.toSet
    val fieldNamesSet = result.schema.fieldNames.toSet
    assert(renameSet.subsetOf(fieldNamesSet),"rename fields should be subset of the fields set")

    renameFields.map{
      case (k,v) =>
        result.withColumnRenamed(k, v)
    }
    result.registerTempTable(tempName)
    sql(s"create table ${params.outputTableName} as select * from $tempName")

  }
}

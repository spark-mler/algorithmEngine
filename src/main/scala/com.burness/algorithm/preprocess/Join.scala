package com.burness.algorithm.preprocess

import com.burness.utils.AbstractParams
import org.apache.spark.SparkContext
import scopt.OptionParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext


/**
 * Created by burness on 16-6-21.
 */
class Join (sc: SparkContext){

  object JoinMethod extends Enumeration {
    type JoinMethod = Value
    val INNER, LEFT, RIGHT, FULL = Value
  }
  import JoinMethod._
  case class Params(leftTableName: String = null,
                    rightTableName: String = null,
                    outputTableName: String = null,
                    joinMethod: JoinMethod = INNER,
                    joinConditions: String = null,
                    leftOutFieldsName: String = null,
                    rightOutFieldsName: String = null)
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
      opt[String]("joinMethod")
        .text("join method")
        .action((x,c) => c.copy(joinMethod = JoinMethod.withName(x)))
      opt[String]("joinConditions")
        .text("join conditions")
        .action((x, c) => c.copy(joinConditions = x))
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
    val leftOutFieldsName = params.leftOutFieldsName.split("\t").map{
      case s =>
        "p1."+s.split(",")(0)+ " as "+s.split(",")(1)
    }.mkString(",")
    val rightOutFieldsName = params.rightOutFieldsName.split("\t").map{
      case s =>
        "p2."+s.split(",")(0)+ " as "+s.split(",")(1)
    }.mkString(",")
    // 处理join条件,先按and 再split =
    val joinConditions = params.joinConditions.split("and").map{
      case s =>
        val sAlias = s.split("=")
        "p1."+sAlias(0).trim+" = "+"p2."+sAlias(1).trim
    }.mkString(" and ")

    val outFiledsName = leftOutFieldsName+","+rightOutFieldsName
    params.joinMethod match {
      case INNER =>
        println(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 inner join " +
          s"${params.rightTableName} p2 on  $joinConditions")
        sql(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 inner join " +
          s"${params.rightTableName} p2 on  $joinConditions")
      case LEFT =>
        println(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 left join " +
          s"${params.rightTableName} p2 on  $joinConditions")
        sql(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 left join " +
          s"${params.rightTableName} p2 on  $joinConditions")
      case RIGHT =>
        println(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 right join " +
          s"${params.rightTableName} p2 on  $joinConditions")
        sql(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 right join " +
          s"${params.rightTableName} p2 on  $joinConditions")
      case FULL =>
        println(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 full outer join " +
          s"${params.rightTableName} p2 on  $joinConditions")
        sql(s"create table ${params.outputTableName} as  select $outFiledsName from ${params.leftTableName} p1 full outer join " +
          s"${params.rightTableName} p2 on  $joinConditions")

    }

  }


}

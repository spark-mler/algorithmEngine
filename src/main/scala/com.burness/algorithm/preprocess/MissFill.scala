//package com.chinac.algorithm.preprocess
//
//import com.com.burness.utils.AbstractParams
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.hive.HiveContext
//import org.apache.spark.sql.types._
//import scopt.OptionParser
//
///**
// * Created by burness on 16-6-27.
// */
//class MissFill(sc:SparkContext) {
////  object OriginVal extends Enumeration {
////    type OriginVal = Value
////    /*
////    NULL1:数值与string的空；
////    NULL2:string;
////    NULL3:NULL和空字符（string）
////    NULLDefine：自定义的string作空
////     */
////
////    val NULL1, NULL2, NULL3, NULLDefine = Value
////  }
//   object ReplaceVal extends Enumeration{
//     type ReplaceVal = Value
//     val MIN, MAX, MEAN, VALDefine = Value
//   }
//
//  import ReplaceVal._
////  import OriginVal._
//  case class Params(inputTableName: String = null,
//                    outputTableName: String = null,
//                    fieldsName: String = null,
//                    replaceVal: ReplaceVal = null,
//                    userDefineReplaceVal: String = null,
//                    isAdvanced: Boolean = false,
//                    config: String = null
//                   )
//    extends AbstractParams[Params]
//
//  def parseParams(args: Array[String]): Params = {
//    val defaultParams = Params()
//    val parser = new OptionParser[Params]("normalize table columns") {
//      head("input table name")
//      opt[String]("inputTableName")
//        .text("input table name")
//        .action((x, c) => c.copy(inputTableName = x))
//      opt[String]("outputTableName")
//        .text("output table name")
//        .action((x,c) => c.copy(outputTableName = x))
//      opt[String]("fieldsName")
//        .text("fields name")
//        .action((x,c) => c.copy(fieldsName = x))
//      opt[String]("replaceVal")
//        .text("replace value")
//        .action((x,c) => c.copy(replaceVal = ReplaceVal.withName(x)))
//      opt[String]("userDefineReplaceValue")
//        .text("user Define Replace value")
//        .action((x,c) => c.copy(userDefineReplaceVal = x))
//      opt[Boolean]("isAdvanced")
//        .text("whether to get advenced config")
//        .action((x,c) => c.copy(isAdvanced = x))
//      opt[String]("config")
//        .text("the config string")
//        .action((x,c) => c.copy(config = x))
//      note(
//        """
//          |For example, the following command runs this app on a synthetic dataset:
//          |
//          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
//          |  examples/target/scala-*/spark-examples-*.jar \
//          |  --algorithm LR --regType L2 --regParam 1.0 \
//          |  data/mllib/sample_binary_classification_data.txt
//          |
//        """.stripMargin
//      )
//    }
//    parser.parse(args, defaultParams) match{
//      case Some(params) =>
//        params
//      case None =>
//        defaultParams
//    }
//  }
//  def run(params: Params): Unit = {
//    val hiveContext = new HiveContext(sc)
//    val numericTypes = Seq()
//
//    import hiveContext.implicits._
//    import hiveContext.sql
//    var inputDF = sql(s"select * from ${params.inputTableName}")
//    if (!params.isAdvanced) {
//      val columnsMap = params.fieldsName.split(",").toSeq.map {
//        case s =>
//          val temp = params.replaceVal match {
//            case MIN =>
//              // 这里必须原本字段为数字型
//              try {
//                val colMin = inputDF.agg(min(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMin, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MIN")
//              }
//
//            case MAX =>
//              // 这里必须原本字段为数字型
//              try {
//                val colMax = inputDF.agg(max(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMax, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MAX")
//              }
//            case MEAN =>
//              // 这里必须原本字段为数字型
//              try {
//                val colMean = inputDF.agg(mean(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMean, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MEAN")
//              }
//            case VALDefine =>
//              //无限制
//              val fieldIndex = inputDF.schema.fieldIndex(s)
//              val fieldType = inputDF.schema.fields(fieldIndex).dataType
//              fieldType match {
//                case LongType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toLong, Seq() :+ s)
//                case IntegerType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toInt, Seq() :+ s)
//                case FloatType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toFloat, Seq() :+ s)
//                case DoubleType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toDouble, Seq() :+ s)
//                case ShortType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toShort, Seq() :+ s)
//                //              case DecimalType => {
//                //                val DeReplaceVal = BigDecimal.apply(params.userDefineReplaceVal)
//                //                inputDF = inputDF.na.fill(DeReplaceVal, Seq():+s)
//                //              }
//                case StringType => inputDF = inputDF.na.fill(params.userDefineReplaceVal, Seq() :+ s)
//              }
//
//
//          }
//      }
//      inputDF.write.saveAsTable(params.outputTableName)
//
//    }
//    else {
//      // 解析config 字段名，min/max/mean/userdefined
//      params.config.split("\t").map {
//        case s =>
//          val sList = s.split(",")
//          val fieldName = sList(0)
//          val replaceMethod = sList(1)
//          replaceMethod match {
//            case MIN =>
//              try {
//                val colMin = inputDF.agg(min(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMin, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MIN")
//              }
//            case MAX =>
//              try {
//                val colMax = inputDF.agg(max(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMax, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MAX")
//              }
//
//            case MEAN =>
//              try {
//                val colMax = inputDF.agg(max(s)).first match {
//                  case Row(x: Double) => x
//                }
//                inputDF = inputDF.na.fill(colMax, Seq() :+ s)
//              }
//              catch {
//                case _ => println(s"check the type of $s column whether support the MAX")
//              }
//            case VALDefine =>
//              val fieldIndex = inputDF.schema.fieldIndex(s)
//              val fieldType = inputDF.schema.fields(fieldIndex).dataType
//              fieldType match {
//                case LongType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toLong, Seq() :+ s)
//                case IntegerType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toInt, Seq() :+ s)
//                case FloatType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toFloat, Seq() :+ s)
//                case DoubleType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toDouble, Seq() :+ s)
//                case ShortType => inputDF = inputDF.na.fill(params.userDefineReplaceVal.toShort, Seq() :+ s)
//                case StringType => inputDF = inputDF.na.fill(params.userDefineReplaceVal, Seq() :+ s)
//
//              }
//          }
//
//
//      }
//      inputDF.write.saveAsTable(params.outputTableName)
//
//    }
//  }
//}

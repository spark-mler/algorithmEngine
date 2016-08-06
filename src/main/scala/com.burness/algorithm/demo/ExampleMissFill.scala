//package com.burness.algorithm.demo
//
//import com.com.burness.algorithm.preprocess.MissFill
//import org.apache.spark.{SparkConf, SparkContext}
//
///**
// * Created by burness on 16-6-29.
// */
//object ExampleMissFill {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("standard example")
//    val sc = new SparkContext(conf)
//    val args_test = Seq("--inputTableName","s_input","--outputTableName","s_output","--standardColumnsName","x1,x2,x3",
//      "--isAddStandardColumns","false").toArray
//
//    val model = new  MissFill(sc)
//    val params = model.parseParams(args_test)
//    model.run(params)
//
//    val args_test2 = Seq("--inputTableName","s_input","--outputTableName","s_output2","--standardColumnsName","x1,x2,x3",
//      "--isAddStandardColumns","true").toArray
//
//    val params2 = model.parseParams(args_test2)
//    model.run(params2)
//  }
//
//}

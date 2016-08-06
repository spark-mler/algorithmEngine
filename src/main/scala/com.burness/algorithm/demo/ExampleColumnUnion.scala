package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.{ColumnUnion, Join}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-22.
 */
object ExampleColumnUnion {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join example")
    val sc = new SparkContext(conf)

    val args_test = Seq("--leftTableName", "CU_left", "--rightTableName", "CU_right", "--outputTableName", "CU_output",
      "--leftOutFieldsName", "x1,x_1\tx2,x_2\tds,ds",
      "--rightOutFieldsName", "x1,x1\tx2,x2").toArray

    val model = new ColumnUnion(sc)
    val params = model.parseParams(args_test)
    model.run(params)
  }
}

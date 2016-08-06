package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.FilterRename
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-21.
 */
object ExampleFilterRename {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("FilterRename example")
    val sc = new SparkContext(conf)
    val args_test = Seq( "--inputTableName", "random_test", "--outputTableName",
      "random_test_output","--filterFields","x1,x2,x4,x5","--filterConditions","x3>10",
      "--renameFields","x1,x_1\tx2,x_2\tx4,x_4\tx5,x_5").toArray


    val model = new FilterRename(sc)
    val params = model.parseParams(args_test)
    model.run(params)

    val args_test2 = Seq( "--inputTableName", "random_test2", "--outputTableName",
      "random_test_output2","--filterFields","x1,x2,x4,x5","--filterConditions","x3>10",
      "--renameFields","x1,x_1\tx2,x_2\tx4,x_4\tx6,x_6").toArray
    val params2 = model.parseParams(args_test2)
    model.run(params2)


  }

}

package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.Union
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-23.
 */
object ExampleUnion {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Join example")
    val sc = new SparkContext(conf)

    val args_test = Seq("--leftTableName", "U_left", "--rightTableName", "U_right", "--outputTableName", "U_output",
      "--removeDeplicate","false").toArray

    val model = new Union(sc)
    val params = model.parseParams(args_test)
    model.run(params)


    val args_test2 = Seq("--leftTableName", "U_left", "--rightTableName", "U_right", "--outputTableName", "U_output2",
      "--removeDeplicate","true").toArray

    val params2 = model.parseParams(args_test2)
    model.run(params2)


  }


}

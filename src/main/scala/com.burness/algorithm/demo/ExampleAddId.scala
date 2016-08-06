package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.AddId
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-24.
 */
object ExampleAddId {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("addId example")
    val sc = new SparkContext(conf)

    val args_test = Seq("--inputTableName", "add_input", "--outputTableName", "add_output",
      "--addIdName","add_id").toArray

    val model = new AddId(sc)
    val params = model.parseParams(args_test)
    model.run(params)
  }

}

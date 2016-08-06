package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.AddId
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-24.
 */
object ExampleAddId {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"addId example")
      .enableHiveSupport()
      .getOrCreate()

    val args_test = Seq("--inputTableName", "openapi_invoke_base", "--outputTableName", "add_output",
      "--addIdName","add_id").toArray

    val model = new AddId(spark)
    val params = model.parseParams(args_test)
    model.run(params)
  }

}

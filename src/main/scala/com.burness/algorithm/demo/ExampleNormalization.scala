package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.Normalization
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-24.
 */
object ExampleNormalization {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("normalization example")
    val sc = new SparkContext(conf)

    val args_test = Seq("--inputTableName", "nor_input", "--outputTableName", "nor_output",
      "--normalizeColumnsName","x1,0.0,1.0\tx2,0.0,1.0\tx3,0.0,1.0","--isAddNormalizeColumns","false").toArray

    val model = new Normalization(sc)
    val params = model.parseParams(args_test)
    model.run(params)

    val args_test2 = Seq("--inputTableName", "nor_input", "--outputTableName", "nor_output2",
      "--normalizeColumnsName","x1,-1.0,1.0\tx2,0.0,1.0\tx3,0.0,1.0","--isAddNormalizeColumns","true").toArray
    val params2 = model.parseParams(args_test2)
    model.run(params2)
  }

}

package com.burness.algorithm.demo

import com.burness.algorithm.preprocess.RandomSampling
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by burness on 16-6-20.
 */
object RandomSamplingExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("RandomSampling example")
    val sc = new SparkContext(conf)
    val args_test = Seq( "--inputTableName", "random_test", "--samplingRatio",
      "0.7","--outputTableName","random_test_output").toArray


    val model = new RandomSampling(sc)
    val params = model.parseParams(args_test)
    model.run(params)

  }

}

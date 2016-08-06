package com.burness.algorithm.demo

import com.burness.algorithm.feature.Pca
import org.apache.spark.sql.SparkSession

/**
  * Created by burness on 16/8/6.
  */
object ExamplePca {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("pca example").enableHiveSupport().getOrCreate()

    val args_test = Seq("--inputTableName", "pca_input", "--outputTableName", "pca_output",
      "--inputCol","f1,f2,f3,f4,f5","--n_components","3").toArray

    val model = new Pca(spark)
    val params = model.parseParams(args_test)
    model.run(params)
  }

}

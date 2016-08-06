package com.burness.algorithm.feature

import com.burness.utils.AbstractParams
import org.apache.spark.ml.feature.{PCA, VectorAssembler}
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{Row, SparkSession}
import scopt.OptionParser


/**
  * Created by burness on 16/8/6.
  */
class Pca(spark: SparkSession) {

  case class Params(inputTableName: String = null,
                    inputCol: String = null,
                    outputTableName: String = null,
//                    outputCol: String = null,
                    n_components: Int = -1
                   )
    extends AbstractParams[Params]

  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("PCA with table") {
      head("process the table with PCA, the data must be all numerical")
      opt[String]("inputTableName")
        .text("input table name")
        .action((x, c) => c.copy(inputTableName = x))
      opt[String]("inputCol")
        .text("the Col with the input table")
        .action((x, c) => c.copy(inputCol = x))
      opt[String]("outputTableName")
        .text("the table after pca")
        .action((x, c) => c.copy(outputTableName = x))
//      opt[String]("outputCol")
//        .text("the col names of the output table")
//        .action((x, c) => c.copy(outputCol = x))
      opt[Int]("n_components")
        .text("the num of components")
        .action((x, c) => c.copy(n_components = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
          |
        """.stripMargin
      )
    }
    parser.parse(args, defaultParams) match {
      case Some(params) =>
        params
      case None =>
        defaultParams
    }
  }

  def run(params: Params): Unit = {
//     read the hive table and Vectors
    println(params.inputTableName)
    val inputTable = spark.sql(s"select * from ${params.inputTableName}")
    val inputColArray = params.inputCol.split(",")
    val assembler = new VectorAssembler()
      .setInputCols(inputColArray)
      .setOutputCol("Features")
    val inputVectors = assembler.transform(inputTable)
    val pca = new PCA()
      .setInputCol("Features")
      .setOutputCol("pcaFeatures")
      .setK(params.n_components)
      .fit(inputVectors)

    val result = pca.transform(inputVectors).select("pcaFeatures")
    result.write.saveAsTable(params.outputTableName)





  }
}

package com.burness.algorithm.model.mllib.classification

import com.burness.utils.AbstractParams
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import scopt.OptionParser


/**
  * Created by burness on 16-5-25.
  */
class SVM(sc: SparkContext) {

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }

  import RegType._

  case class Params(
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     regType: RegType = L1,
                     regParam: Double = 0.0,
                     miniBatchFraction: Double = 1.0,
                     modelSavePath: String = null,
                     saveModel: Boolean = false
                   ) extends AbstractParams[Params]


  def parseParams(args: Array[String]): Params = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("BinaryClassification With SVM") {
      head("Binary Classification with SVM Params parse")
      opt[Int]("numIterations")
        .text("number of iterations")
        .action((x, c) => c.copy(numIterations = x))
      opt[Double]("stepSize")
        .text("initial step size (ignored by logistic regression), " +
          s"default: ${defaultParams.stepSize}")
        .action((x, c) => c.copy(stepSize = x))
      opt[String]("regType")
        .text(s"regularization type (${RegType.values.mkString(",")}), " +
          s"default: ${defaultParams.regType}")
        .action((x, c) => c.copy(regType = RegType.withName(x)))
      opt[Double]("regParam")
        .text(s"regularization parameter, default: ${defaultParams.regParam}")
        .action((x, c) => c.copy(regParam = x))
      opt[Double]("miniBatchFraction")
        .text("miniBatch Fraction")
        .action((x, c) => c.copy(miniBatchFraction = x))
      opt[Boolean]("saveModel")
        .text("whether to save the model")
        .action((x, c) => c.copy(saveModel = x))
      opt[String]("modelSavePath")
        .text("model save path")
        .action((x, c) => c.copy(modelSavePath = x))
      note(
        """
          |For example, the following command runs this app on a synthetic dataset:
          |
          | bin/spark-submit --class org.apache.spark.examples.mllib.BinaryClassification \
          |  examples/target/scala-*/spark-examples-*.jar \
          |  --algorithm LR --regType L2 --regParam 1.0 \
          |  data/mllib/sample_binary_classification_data.txt
        """.stripMargin)
    }
    parser.parse(args, defaultParams) match {
      case Some(params) =>
        params
      case None =>
        defaultParams
    }
  }

  def run(params: Params, training: RDD[LabeledPoint], initialWeights: Vector): SVMModel = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val nClasses = training.map {
      case LabeledPoint(label, features) =>
        label
    }.distinct().count()



    val model = {
      require(nClasses == 2, "BinarySVM SGD didn't support nClasses > 2")
      val algorithm = new SVMWithSGD()
      algorithm.optimizer
        .setNumIterations(params.numIterations)
        .setUpdater(updater)
        .setRegParam(params.regParam)
        .setMiniBatchFraction(params.miniBatchFraction)
      algorithm.run(training).clearThreshold()
    }
    if (params.saveModel) {
      model.save(sc, params.modelSavePath)
    }
    model
  }

  def run(params: Params, training: RDD[LabeledPoint]): SVMModel = {
    val updater = params.regType match {
      case L1 => new L1Updater()
      case L2 => new SquaredL2Updater()
    }

    val nClasses = training.map {
      case LabeledPoint(label, features) =>
        label
    }.distinct().count()

    val model = {
      require(nClasses == 2, "SGD didn't support nClasses > 2")
      val algorithm = new SVMWithSGD()
      algorithm.optimizer
        .setNumIterations(params.numIterations)
        .setUpdater(updater)
        .setRegParam(params.regParam)
      algorithm.run(training).clearThreshold()

    }
    if (params.saveModel) {
      model.save(sc, params.modelSavePath)
    }
    model
  }

  def loadModel(path: String): SVMModel = {
    val model = SVMModel.load(sc, path)
    model
  }
}

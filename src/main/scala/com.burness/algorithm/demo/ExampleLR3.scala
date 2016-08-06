package com.burness.algorithm.demo

import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.optimization.L1Updater

/**
  * Created by burness on 16/5/18.
  */
object ExampleLR3 {

  import org.apache.spark.{SparkConf, SparkContext}

  // $example on$
  import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
  import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
  import org.apache.spark.mllib.regression.LabeledPoint
  import org.apache.spark.mllib.util.MLUtils

  // $example off$

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("BinaryClassificationMetricsExample").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // $example on$
    // Load training data in LIBSVM format
//    val dataFile = getClass.getResource("/data/sample_binary_classification_data.txt").getPath
    val dataFile = getClass.getResource("/data/real-sim").getPath

    val data = MLUtils.loadLibSVMFile(sc, dataFile)
    val data2 = data.map{
      case LabeledPoint(label, features)=>
        if(label.equals(-1.0))
          LabeledPoint(0.0, features)
        else
          LabeledPoint(label, features)
    }
    // Split data into training (60%) and test (40%)
    val Array(training, test) = data2.randomSplit(Array(0.6, 0.4), seed = 11L)
    training.cache()

    // Run training algorithm to build the model
    val algorithm = new LogisticRegressionWithLBFGS()
    algorithm.optimizer.setUpdater(new L1Updater()).setRegParam(0.01).setNumIterations(100)
    val model = algorithm
      .run(training)

    // Clear the prediction threshold so the model will return probabilities

    // Compute raw scores on the test set
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // Precision by threshold
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    // $example off$
  }

  // scalastyle:on println

}

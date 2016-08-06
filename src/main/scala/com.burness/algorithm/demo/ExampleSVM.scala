package com.burness.algorithm.demo

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}
import com.burness.algorithm.model.mllib.classification.SVM
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
/**
  * Created by burness on 16-5-25.
  */
object ExampleSVM {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("LR example").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //    val spark = SparkSession.builder.config(conf)
    val dataFile  = getClass.getResource("/data/real-sim").getPath
    val data = MLUtils.loadLibSVMFile(sc, dataFile)
    val dataRows = data.count()
    val dataFeatureNum = data.take(1)(0).features.size
    println(s"feature size: $dataFeatureNum")
    println(s"data rows : $dataRows")
    println(s"data features")

    val data2 = data.map{
      case LabeledPoint(label, features)=>
        if(label.equals(-1.0))
          LabeledPoint(0.0, features)
        else
          LabeledPoint(label, features)
    }

    val splits = data2.randomSplit(Array(0.8,0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest")

    val args_test = Seq( "--regType", "L1", "--regParam",
      "0.0","--numIterations","10","--miniBatchFraction","0.8").toArray

    val model = new SVM(sc)
    val params = model.parseParams(args_test)
    println("--------------------")
    println(params)
    val start = System.nanoTime()
    val trainedModel = model.run(params,training)
    //    trainedModel.weights.toArray.foreach(println)
    val timeMs = System.nanoTime() - start
    println(s"training time : $timeMs ns")
    val prediction = trainedModel.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")


    val args_test2 = Seq( "--regType", "L1", "--regParam",
      "0.0","--numIterations","10","--miniBatchFraction","0.8","--saveModel","true","--modelSavePath","/home/burness/test").toArray

    val modelSave = new SVM(sc)
    val paramsSave = modelSave.parseParams(args_test2)
    println("-"*30)
    val start2 = System.nanoTime()
    val trainedSVMModel = modelSave.run(paramsSave, training)
    val timeMs2 = System.nanoTime() - start2
    println(s"training time : $timeMs ns")
    val prediction2 = trainedSVMModel.predict(test.map(_.features))
    val predictionAndLabel2 = prediction2.zip(test.map(_.label))

    val metrics2 = new BinaryClassificationMetrics(predictionAndLabel2)

    println(s"Test areaUnderPR = ${metrics2.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics2.areaUnderROC()}.")

    val modelLoad = new SVM(sc).loadModel("/home/burness/test")
    assert(modelLoad.weights == trainedSVMModel.weights)
    assert(modelLoad.intercept == trainedSVMModel.intercept)




  }

}

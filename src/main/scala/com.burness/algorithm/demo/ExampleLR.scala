package com.burness.algorithm.demo

import com.burness.algorithm.model.mllib.classification.LogisticRegression
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by burness on 16/5/17.
  */
object ExampleLR {

  private def checkModelsEqual(a: LogisticRegressionModel, b: LogisticRegressionModel): Unit = {
    assert(a.weights == b.weights)
    assert(a.intercept == b.intercept)
    assert(a.numClasses == b.numClasses)
    assert(a.numFeatures == b.numFeatures)
    assert(a.getThreshold == b.getThreshold)
  }

  def main(args: Array[String]) {
//    val conf = new SparkConf().setAppName("LR example").setMaster("local[*]")
    val conf = new SparkConf().setAppName("LR example")
    val sc = new SparkContext(conf)
//    val spark = SparkSession.builder.config(conf)
//    val dataFile  = getClass.getResource("/data/real-sim").getPath
      val dataFile = "/user/spark/algorithm/classification/real-sim"
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
    data2.take(20).foreach(println)
//    val data3 = data2.randomSplit(Array(0.001, 0.999))
    val splits = data2.randomSplit(Array(0.8,0.2))
    // 对feature做一个降维处理
    val training = splits(0)
    val test = splits(1)

    data.unpersist()
    data2.unpersist()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest")

    val args_test = Seq( "--regType", "L2", "--regParam",
      "0.01","--numIterations","10","--optimizer","LBFGS").toArray



    val model = new LogisticRegression(sc)
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



    val modelInitial = new  LogisticRegression(sc)
    val paramsInitial = modelInitial.parseParams(args_test)
    val initialVectors = Vectors.dense(new Array[Double](dataFeatureNum))
    println("initial vectors")
//    initialVectors.toArray.foreach(println)
    println("--------------------")
    println(params)
    val startInitial = System.nanoTime()
    val trainModelInitial = modelInitial.run(paramsInitial, training, initialVectors)
    val timeMsInitial = System.nanoTime() - start
    println(s"training time :$timeMsInitial ns")
    val predictionInitial = trainModelInitial.predict(test.map(_.features))
    val predictionAndLabelInitial = predictionInitial.zip(test.map(_.label))
    val metricsInitial = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metricsInitial.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metricsInitial.areaUnderROC()}.")


    // multi class with iris data, only support the LBFGS
//    val irisFile = getClass.getResource("/data/iris.data").getPath
    val irisFile = "/user/spark/algorithm/classification/iris.data"
    val irisData = sc.textFile(irisFile)
    val irisLabeledPoint = irisData.map{
      case s=>
        val sList = s.split(",")
        val label = sList.last.toDouble
        val featuresArray = sList.slice(0,sList.length-1).map{
          case s=>
            s.toDouble
        }
        val features = Vectors.dense(featuresArray)
        LabeledPoint(label, features)
    }
    val irisSplit = irisLabeledPoint.randomSplit(Array(0.8, 0.2))
    val irisTraining = irisSplit(0).cache()
    val irisTest = irisSplit(1).cache()
    val numIrisTraining = irisTraining.count()
    val numIrisTest = irisTest.count()

    val args_iris = Seq("--numIterations","100","--optimizer","LBFGS").toArray
    val irisModel = new LogisticRegression(sc)
    val irisParams = irisModel.parseParams(args_iris)
    val trainModelIris = irisModel.run(irisParams, irisTraining)
    val predictionIris = trainModelIris.predict(irisTest.map(_.features))
    val predictionAndLabelIris = predictionIris.zip(irisTest.map(_.label))

    val metricsIris = new MulticlassMetrics(predictionAndLabelIris)
    println(s"Test fMeasure =${metricsIris.fMeasure}" )
    println(s"Test precision =${metricsIris.precision}" )
    println(s"Test recall =${metricsIris.recall}")
    println(metricsIris.confusionMatrix)

    // multi class with iris data, only support the LBFGS, save the lr model
    val argsIrisSave = Seq("--numIterations","100","--optimizer","LBFGS","--saveModel","true","--modelSavePath",
      "/user/spark/algorithm/model").toArray
    val irisModelSave = new LogisticRegression(sc)
    val irisParamsSave = irisModelSave.parseParams(argsIrisSave)
    val trainModelIrisSave = irisModelSave.run(irisParamsSave, irisTraining)
    val predictionIrisSave = trainModelIrisSave.predict(irisTest.map(_.features))
    val predictionAndLabelIrisSave = predictionIrisSave.zip(irisTest.map(_.label))

    val metricsIrisSave = new MulticlassMetrics(predictionAndLabelIrisSave)
    println(s"Test fMeasure =${metricsIrisSave.fMeasure}" )
    println(s"Test precision =${metricsIrisSave.precision}" )
    println(s"Test recall =${metricsIrisSave.recall}")
    println(metricsIrisSave.confusionMatrix)


    // check the loadModel
    val irisModelLoad = new LogisticRegression(sc).loadModel("/user/spark/algorithm/model")

    checkModelsEqual(irisModelLoad, trainModelIrisSave)



  }

}

//package com.chinac.algorithm.demo
//
////import com.com.burness.algorithm.preprocess.Sample
//import com.com.burness.algorithm.preprocess.Sample
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.{SparkContext, SparkConf}
//
///**
//  * Created by burness on 16/5/12.
//  */
//object ExampleSample {
//  def main(args: Array[String]) {
//    Logger.getRootLogger.setLevel(Level.WARN)
//    val n = 37254
//    val config = new SparkConf().setAppName("Sample test").setMaster("local[2]").set("spark.logConf","true")
//    val sc = new SparkContext(config)
//    val sqlContext = new SQLContext(sc)
//    val data1 = sqlContext.emptyDataFrame
//    import sqlContext.implicits._
//    val data = sc.parallelize(1 to n, 2).toDF("id")
//    val dataSampler = new Sample(data, sqlContext)
//
//
//    // 不放回\0.1采样概率\seed=13
//    val dataAfterSampling = dataSampler.parse(false, 0.1, 13)
//    val dataNumAfterSampling = dataAfterSampling.count()
//    println(s"The num of the data after sampling is $dataNumAfterSampling")
//    // 不放回\0.1采样概率\不指定seed
//    val dataAfterSampling2 = dataSampler.parse(false, 0.1)
//    val dataNumAfterSampling2 = dataAfterSampling2.count()
//    println(s"The num of the data after sampling is $dataNumAfterSampling2")
//    // 不放回\采样个数为50\seed为13
//    val dataAfterSampling3 = dataSampler.parse(false, 50,13)
//    val dataNumAfterSampling3 = dataAfterSampling3.count
//    println(s"The num of the data after sampling is $dataNumAfterSampling3")
//
//    // 不放回\采样个数为50\不指定seed
//    val dataAfterSampling4 = dataSampler.parse(false, 50)
//    val dataNumAfterSampling4 = dataAfterSampling4.count
//    println(s"The num of the data after sampling is $dataNumAfterSampling4")
//
//    // 不放回\0.1采样概率\seed=13
//    val dataAfterSampling5 = dataSampler.parse(true, 0.1, 13)
//    val dataNumAfterSampling5 = dataAfterSampling.count()
//    println(s"The num of the data after sampling is $dataNumAfterSampling5")
//    // 不放回\0.1采样概率\不指定seed
//    val dataAfterSampling6 = dataSampler.parse(true, 0.1)
//    val dataNumAfterSampling6 = dataAfterSampling6.count()
//    println(s"The num of the data after sampling is $dataNumAfterSampling6")
//    // 不放回\采样个数为50\seed为13
//    val dataAfterSampling7 = dataSampler.parse(true, 50,13)
//    val dataNumAfterSampling7 = dataAfterSampling7.count
//    println(s"The num of the data after sampling is $dataNumAfterSampling7")
//
//    // 不放回\采样个数为50\不指定seed
//    val dataAfterSampling8 = dataSampler.parse(true, 50)
//    val dataNumAfterSampling8 = dataAfterSampling8.count
//    println(s"The num of the data after sampling is $dataNumAfterSampling8")
//
//    // 分层采样
////    val dataStratifiedSample = sc.textFile(getClass.getResource("/person/person.txt").getPath)
////    val dataStratifiedSampleRow = dataStratifiedSample.map(_.split("")).map(p=>Row(p(0),p(1),p(2).toInt))
////    val schema = StructType(
////
////    )
//    sc.stop()
//  }
//
//
//}

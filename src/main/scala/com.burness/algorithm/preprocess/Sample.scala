//package com.chinac.algorithm.preprocess
//
//import org.apache.spark.sql.{DataFrame, SQLContext}
//
///**
//  * Created by burness on 16/5/12.
//  */
///**
//  * This class does for sampling with the DataFrame or RDD,including different mode
//  */
//class Sample(data: DataFrame, sqlContext: SQLContext) {
//
//  /**
//    *
//    * @param withReplacement whether with replacement
//    * @param fraction the ratio of the sampling
//    * @return
//    */
//  def parse(withReplacement: Boolean, fraction: Double): DataFrame = {
//    data.sample(withReplacement = withReplacement, fraction = fraction)
//  }
//
//
//  def parse(withReplacement: Boolean, fraction: Double, seed: Long): DataFrame = {
//    data.sample(withReplacement = withReplacement, fraction = fraction, seed = seed)
//  }
//
//  /**
//    *
//    * @param withReplacement
//    * @param sampleNum
//    * @return
//    */
//  def parse(withReplacement: Boolean, sampleNum: Int): DataFrame = {
//    data.takeSample(false,sampleNum)
//  }
//
//
//  def parse(withReplacement: Boolean, sampleNum: Int, seed: Long): DataFrame = {
//    data.takeSample(false,sampleNum, seed)
//  }
//
//
//
//
//
//
//}

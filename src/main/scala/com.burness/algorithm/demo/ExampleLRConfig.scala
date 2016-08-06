package com.burness.algorithm.demo

import com.burness.utils.{SortMap, ReadXML}
import scala.collection.mutable.Map

/**
  * Created by burness on 16/5/19.
  */
object ExampleLRConfig {
  def main(args: Array[String]) {
    val readXML = new ReadXML(getClass.getResource("/config/lrJobConf.xml").getPath)
    val xmlMap = readXML.read().map{
      case (pipeLineId, list)=>
        (pipeLineId.toInt, list)
    }.toMap
    println(xmlMap)

  }
}

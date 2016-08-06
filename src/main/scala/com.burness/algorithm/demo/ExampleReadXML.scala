package com.burness.algorithm.demo

import com.burness.utils.ReadXML
/**
  * Created by burness on 16/5/11.
  */
object ExampleReadXML {
  def main(args: Array[String]) {
    val readXML = new ReadXML(getClass.getResource("/jobConf.xml").getPath)
    val xmlMap = readXML.read()

    println(xmlMap)
    val pipelineNum = readXML.getpipelineNum()
    println(s"pipeline Num: $pipelineNum")
    val jobNameList = readXML.getJobNameMap
    jobNameList.foreach(println(_))
  }

}

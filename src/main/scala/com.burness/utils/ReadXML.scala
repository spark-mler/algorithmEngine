package com.burness.utils

import scala.xml.XML
import scala.collection.mutable.Map
/**
  * Created by burness on 16/5/11.
  */

/**
  * ReadXML is a class to read XML Config for our job
  */
class ReadXML(file: String) {
//  var jobNum = -1
  var pipelineNum = -1
  var jobNameMap = Map.empty[String, List[String]]
  /**
    * read the config and return a List[scala.collection.mutable.HashMap[String,String]]
    * @return a List[scala.collection.mutable.HashMap[String,String]]
    *
   */
  def read(): Map[String,List[Map[String, String]]] = {
    val xmlData = XML.loadFile(file)
    pipelineNum = (xmlData \\ "pipeline").size
    val result = Map.empty[String,List[Map[String, String]]]
    //    println(s"job num : $jobNum")
    val pipelineField = (xmlData \\ "pipeline").map {
      case f =>
        val pipelineJobName = scala.collection.mutable.ListBuffer.empty[String]
        val pipelineId = (f \ "pipelineId").text
//        println(pipelineId)
        val tempList = scala.collection.mutable.ListBuffer.empty[Map[String,String]]

        val jobField = (f \\ "jobNode").map {
        case s =>
          var tempMap = Map.empty[String, String]
          val jobName = (s \ "jobName").text
          pipelineJobName += jobName
          tempMap += ("jobName" -> jobName)
          val paramsName = (s \\ "kvParam").map {
            case ss =>
              val paramName = ss \ "@name"
              val paramVal = ss.text
              tempMap(paramName.toString()) = paramVal

          }
          tempList += tempMap
      }
        result(pipelineId) = tempList.toList
        jobNameMap(pipelineId) = pipelineJobName.toList
    }
    return result
  }

  /**
    * get the job num in the config file
    * @return
    */
  def getpipelineNum():Int = {
    return pipelineNum
  }

  /**www.
    * @
    * @return
    */

  def getJobNameMap(): Map[String, List[String]] = {
    return jobNameMap
  }



}

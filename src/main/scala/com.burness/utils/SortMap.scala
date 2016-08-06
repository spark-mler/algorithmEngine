package com.burness.utils

import scala.collection.immutable.ListMap
import scala.collection.mutable.Map

/**
  * Created by burness on 16/5/19.
  */
class SortMap {
  def sortByPipelineId(map:scala.collection.immutable.Map[Int,List[Map[String, String]]],ascend: Boolean): ListMap[Int,List[Map[String, String]]] ={
    if(ascend){
      ListMap(map.toSeq.sortBy(_._1):_*)
    }else{
      ListMap(map.toSeq.sortWith(_._1 > _._1):_*)
    }
  }

}

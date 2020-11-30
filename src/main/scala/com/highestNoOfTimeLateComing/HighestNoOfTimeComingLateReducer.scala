package com.highestNoOfTimeLateComing

import java.lang
import java.time.LocalTime

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.JavaConversions._



class HighestNoOfTimeComingLateReducer extends Reducer[Text,Text,Text,Text] {
  var map: mutable.LinkedHashMap[String,String] = _
  var signInTime: LocalTime = _
  var recordMap : mutable.LinkedHashMap[String,Int] = _

  override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    signInTime = LocalTime.parse("09:00:00")
    map = new mutable.LinkedHashMap[String,String]()
    recordMap = new mutable.LinkedHashMap[String,Int]()
  }
  override
  def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val keyData = key.toString.split(",")
    val newKeyFields = keyData(0)+","+keyData(1)
    var value: String = null
    for (element <- values) {
      value = element.toString
    }
    if(!map.contains(newKeyFields)) {
      map.put(newKeyFields,value )
    }
  }
  def compareTime(time: String): Int ={
    try {
    val signedTime = LocalTime.parse(time)
    signedTime.compareTo(signInTime)
  }
  catch {
    case ex:Exception =>
      println(ex.printStackTrace())
      throw new Exception
  }
  }

  def findLateByTime (map: mutable.LinkedHashMap[String,String]): Int = {
    val one = 1
    map.foreach { time =>
      val status = compareTime(time._2)
      val userName = time._1.split(",")
      val userID = userName(0)
      if(status > 0) {
        if(recordMap.contains(userID)) {
          val record = recordMap.getOrElse(userID,0)
          val totalDays = record + one
          recordMap.put(userID,totalDays)
        }
        else {
          recordMap.put(userID,one)
        }
      }
    }
    1
  }
  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val status = findLateByTime(map)
    if (status == 1) {
      val sortByLateComers = ListMap(recordMap.toSeq.sortWith(_._2 > _._2): _*)
      sortByLateComers.foreach { user =>
        context.write(new Text(user._1),new Text("Late By "+user._2+" times"))
      }
    }
  }
}

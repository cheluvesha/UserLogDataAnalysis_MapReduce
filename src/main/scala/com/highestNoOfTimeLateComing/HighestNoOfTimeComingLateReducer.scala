package com.highestNoOfTimeLateComing

import java.lang
import java.time.LocalTime
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.JavaConversions._

/***
 * Reducer class aggregates values wrt customized key and calculates lowest average hour
 */

class HighestNoOfTimeComingLateReducer extends Reducer[Text,Text,Text,Text] {
  var map: mutable.LinkedHashMap[String,String] = _
  var signInTime: LocalTime = _
  var recordMap : mutable.LinkedHashMap[String,Int] = _
  val one = 1
  val zero = 0
  /***
   * setup method initializes instance variables before reduce() method executes
   * @param context - Reducer[Text, Text, Text, Text]#Context
   */
  override def setup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    signInTime = LocalTime.parse("09:00:00")
    map = new mutable.LinkedHashMap[String,String]()
    recordMap = new mutable.LinkedHashMap[String,Int]()
  }
  /***
   * reduce() accepts Key and Value From Mapper class and Process the User Data as Defined
   * @param key Text
   * @param values lang.Iterable[Text]
   * @param context Reducer[Text, Text, Text, Text]#Context
   */
  override
  def reduce(key: Text, values: lang.Iterable[Text], context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val keyData = key.toString.split(",")
    val newKeyFields = keyData(zero)+","+keyData(one)
    var value: String = null
    for (element <- values) {
      value = element.toString
    }
    if(!map.contains(newKeyFields)) {
      map.put(newKeyFields,value )
    }
  }

  /***
   * Compares Time by using LocalTime class
   * @param time String
   * @return Int
   */
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

  /***
   * Checks status and decides whether its a late or early
   * @param map mutable.LinkedHashMap[String,String]
   * @return Int
   */
  def findLateByTime (map: mutable.LinkedHashMap[String,String]): Int = {
    map.foreach { time =>
      val status = compareTime(time._2)
      val userName = time._1.split(",")
      val userID = userName(zero)
      if(status > zero) {
        if(recordMap.contains(userID)) {
          val record = recordMap.getOrElse(userID,zero)
          val totalDays = record + one
          recordMap.put(userID,totalDays)
        }
        else {
          recordMap.put(userID,one)
        }
      }
    }
    one
  }
  /**
   * Before exiting the task writes the data into given context by processing as defined
   * @param context Reducer[Text, Text, Text, Text]#Context
   */
  override def cleanup(context: Reducer[Text, Text, Text, Text]#Context): Unit = {
    val status = findLateByTime(map)
    if (status == one) {
      val sortByLateComers = ListMap(recordMap.toSeq.sortWith(_._2 > _._2): _*)
      sortByLateComers.foreach { user =>
        context.write(new Text(user._1),new Text("Late By "+user._2+" times"))
      }
    }
  }
}

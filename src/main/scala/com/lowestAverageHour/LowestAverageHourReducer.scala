package com.lowestAverageHour


import java.lang
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class LowestAverageHourReducer extends Reducer[Text,IntWritable,Text,Text] {
  var map: mutable.LinkedHashMap[String, ListBuffer[Int]] = _
  var avgTimeMap: mutable.LinkedHashMap[String, Double] = _
  var days = 0
  override def setup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    map = new mutable.LinkedHashMap[String, ListBuffer[Int]]()
    avgTimeMap = new mutable.LinkedHashMap[String, Double]
    days = 6
  }

  override
  def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val keyData = key.toString.split(":")
    val keyName = keyData(0) + ":" + keyData(1)
    if (map.contains(keyName)) {
      val listData = map.get(keyName).orNull
      for (value <- values) {
        val newList = listData ++ List(value.get())
        map.put(keyName, newList)
      }
    }
    else {
      map.put(keyName, ListBuffer())
    }
  }

  def checkForZeros(arrayMins: Array[Int]): Int = {
    var zeroCount = 1
    var countedZero = 0
    var mins = 0
    while (mins < arrayMins.length - 1) {
      if ((arrayMins(mins) == 0) && (arrayMins(mins + 1) == 0 )) {
          zeroCount += 1
        }
        else {
          if(zeroCount >= 6){
            countedZero += zeroCount
            zeroCount = 1
          }
          else {
            zeroCount = 1
          }
        }
      mins += 1
    }
    if(zeroCount >= 6) {
      countedZero += zeroCount
    }
    countedZero
  }

  def evaluateTime(key: String, durationData: ListBuffer[Int]): (String, Double) = {
    try {
      val keyName = key.split(":")
      val arrayMins = durationData.toArray
      val zeroCounted = checkForZeros(arrayMins)
      val overallMins = arrayMins.length * 5
      val totalMins = overallMins - (zeroCounted * 5).toDouble
      val hour: Double = totalMins / 60
      val roundedHours = BigDecimal(hour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (keyName(0), roundedHours)
    }
    catch {
      case ex:Exception =>
        println(ex.printStackTrace())
        throw  new Exception("")
    }
  }

  override def cleanup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    map.foreach {
      x =>
        val userData = evaluateTime(x._1, x._2)
        if(avgTimeMap.contains(userData._1)) {
          val time = avgTimeMap.getOrElse(userData._1,Double).asInstanceOf[Double]
          val t = time + userData._2

          avgTimeMap.put(key = userData._1,value = t )
        }
        else {
          avgTimeMap.put(userData._1, userData._2)
        }
    }
    val sortedData: ListMap[String, Double] = ListMap(avgTimeMap.toSeq.sortWith(_._2 < _._2): _*)
    sortedData.foreach { keyValue =>
      val avgTime = BigDecimal(keyValue._2/days).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      context.write(new Text(keyValue._1), new Text("  -  " + avgTime + " Hours"))
    }
  }
}

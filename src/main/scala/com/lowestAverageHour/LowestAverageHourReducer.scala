package com.lowestAverageHour
import java.lang
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/***
 * Reducer class aggregates values wrt customized key and calculates lowest average hour
 */

class LowestAverageHourReducer extends Reducer[Text,IntWritable,Text,Text] {

  // mutable.LinkedHashMap[String, ListBuffer[Int] - Customized UserData Key and Value List
  var map: mutable.LinkedHashMap[String, ListBuffer[Int]] = _
  // mutable.LinkedHashMap[String, Double] - Stores UserID as Key and Hour as Value
  var avgTimeMap: mutable.LinkedHashMap[String, Double] = _
  var days = 6
  var one = 1
  var minutes = 5
  var idleLimit = 6
  var zero = 0

  /***
   * setup method initializes instance variables before reduce() method executes
   * @param context - Reducer[Text, IntWritable, Text, Text]#Context
   */
  override def setup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    map = new mutable.LinkedHashMap[String, ListBuffer[Int]]()
    avgTimeMap = new mutable.LinkedHashMap[String, Double]

  }

  /***
   * reduce() accepts Key and Value From Mapper class and Process the User Data as Defined
   * @param key Text
   * @param values lang.Iterable[IntWritable]
   * @param context Reducer[Text, IntWritable, Text, Text]#Context
   */
  override
  def reduce(key: Text, values: lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    val keyData = key.toString.split(":")
    val keyName = keyData(0) + ":" + keyData(1)
    var timeData = zero
    for(value <- values) {
      timeData = value.get()
    }
    if (map.contains(keyName)) {
      val listData = map.get(keyName).orNull
      val newList = listData ++ List(timeData)
      map.put(keyName, newList)
    }
    else {
      map.put(keyName, ListBuffer(timeData))
    }
  }

  /***
   * Checks the List for Sequence of Zeros which has to be continuous 6 or more.
   * @param arrayMins Array[Int]
   * @return Int
   */
  def checkForZeros(arrayMins: Array[Int]): Int = {
    var zeroCount = one
    var countedZero = zero
    var mins = zero
    while (mins < arrayMins.length - one) {
      if ((arrayMins(mins) == zero) && (arrayMins(mins + one) == zero )) {
        zeroCount += one
      }
      else {
        if(zeroCount >= idleLimit){
          countedZero += zeroCount
          zeroCount = one
        }
        else {
          zeroCount = one
        }
      }
      mins += one
    }
    if(zeroCount >= idleLimit) {
      countedZero += zeroCount
    }
    countedZero
  }

  /***
   * Evaluates the time according to 1's and 0's in the List
   * @param key String
   * @param durationData ListBuffer[Int]
   * @return (String, Double)
   */
  def evaluateTime(key: String, durationData: ListBuffer[Int]): (String, Double) = {
    try {
      val keyName = key.split(":")
      val arrayMins = durationData.toArray
      val zeroCounted = checkForZeros(arrayMins)
      val overallMins = arrayMins.length * minutes
      val totalMins = overallMins - (zeroCounted * minutes).toDouble
      val hour: Double = totalMins / 60
      val roundedHours = BigDecimal(hour).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
      (keyName(zero), roundedHours)
    }
    catch {
      case ex:Exception =>
        println(ex.printStackTrace())
        throw  new Exception("")
    }
  }

  /**
   * Before exiting the task writes the data into given context by processing as defined
   * @param context Reducer[Text, IntWritable, Text, Text]#Context
   */
  override def cleanup(context: Reducer[Text, IntWritable, Text, Text]#Context): Unit = {
    map.foreach {
      x =>
        val userData = evaluateTime(x._1, x._2)
        if(avgTimeMap.contains(userData._1)) {
          val time = avgTimeMap.getOrElse(userData._1,Double).asInstanceOf[Double]
          val totalTime = time + userData._2

          avgTimeMap.put(key = userData._1,value = totalTime )
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
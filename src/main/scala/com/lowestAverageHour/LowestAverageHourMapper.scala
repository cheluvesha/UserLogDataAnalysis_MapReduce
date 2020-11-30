package com.lowestAverageHour


import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/***
 * Mapper class Which Redirects Text, IntWritable to Reducer Class
 */
class LowestAverageHourMapper extends Mapper[Object,Text,Text,IntWritable] {
  val outputKey = new Text()
  val one = 1

  /***
   * Mapper method reads data and redirects to Reduce class as Key and Value Pair
   * @param key Object
   * @param value Text
   * @param context Mapper[Object, Text, Text, IntWritable]#Context
   */
  override
  def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
    val userLogData = value.toString.split(",")
    val dateTime = userLogData(0)
    val date = dateTime.substring(0,dateTime.indexOf(' '))
    val time =  dateTime.substring(dateTime.indexOf(' '))
    val keyBoardHits: Double = userLogData(41).toDouble
    val mouseClicks: Double = userLogData(42).toDouble
    val engineerID: String = userLogData(40)
    outputKey.set(engineerID+":"+date+":"+time)
    var num = 0
    if((keyBoardHits >= 0) && (mouseClicks > 0)) {
      num = one
    }
    else if ((keyBoardHits > 0) && (mouseClicks >= 0)) {
      num = one
    }
    context.write(outputKey,new IntWritable(num))
  }
}

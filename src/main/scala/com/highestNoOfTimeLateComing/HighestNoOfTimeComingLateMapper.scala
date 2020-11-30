package com.highestNoOfTimeLateComing

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

/***
 * Mapper class Which Redirects Text, IntWritable to Reducer Class
 */
class HighestNoOfTimeComingLateMapper extends Mapper[Object,Text,Text,Text] {
  val outputKey = new Text()
  val outputValue = new Text()
  /***
   * Mapper method reads data and redirects to Reduce class as Key and Value Pair
   * @param key Object
   * @param value Text
   * @param context Mapper[Object, Text, Text, Text]#Context
   */
  override
  def map(key: Object, value: Text, context: Mapper[Object, Text, Text, Text]#Context): Unit = {
    val userLogData = value.toString.split(",")
    val dateTime = userLogData(0)
    val date = dateTime.substring(0,dateTime.indexOf(' '))
    val time =  dateTime.substring(dateTime.indexOf(' ')+1)
    val engineerID: String = userLogData(40)
    outputKey.set(engineerID+","+date+","+time)
    outputValue.set(time)
    context.write(outputKey,outputValue)
  }
}

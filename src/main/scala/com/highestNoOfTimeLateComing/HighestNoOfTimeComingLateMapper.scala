package com.highestNoOfTimeLateComing

import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class HighestNoOfTimeComingLateMapper extends Mapper[Object,Text,Text,Text] {
  val outputKey = new Text()
  val outputValue = new Text()
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

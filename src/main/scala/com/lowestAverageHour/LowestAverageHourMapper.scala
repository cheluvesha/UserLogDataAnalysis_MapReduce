package com.lowestAverageHour


import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class LowestAverageHourMapper extends Mapper[Object,Text,Text,IntWritable] {
  val outputKey = new Text()
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
      num = 1
    }
    else if ((keyBoardHits > 0) && (mouseClicks >= 0)) {
      num = 1
    }
    context.write(outputKey,new IntWritable(num))
  }
}

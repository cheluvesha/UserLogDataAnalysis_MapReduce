package com.highestNoOfIdleHrs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

/***
 * Map Reduce Driver Class
 * Declares the Mapper and Reducer class
 * Declares and Initializes File Input and Output Path
 */
object HighestNoOfIdleHours {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job: Job = new Job(conf, "Highest Idle Hours")
    job.setJarByClass(classOf[HighestNoOfIdleHoursMapper])
    job.setMapperClass(classOf[HighestNoOfIdleHoursMapper])
    job.setReducerClass(classOf[HighestNoOfIdleHoursReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output_HighIdleHrs"))
    job.waitForCompletion(true)
  }
}
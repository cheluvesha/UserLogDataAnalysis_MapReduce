package com.highestAverageHour


/***
 * Dependency Used Hadoop - client to perform MapReduce
 */
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
object HighestAverageHour {

  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job: Job = new Job(conf, "Highest Average Hours")
    job.setJarByClass(classOf[HighestAverageHourMapper])
    job.setMapperClass(classOf[HighestAverageHourMapper])
    job.setReducerClass(classOf[HighestAverageHourReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output_HighAvgHrs"))
    job.waitForCompletion(true)
  }
}
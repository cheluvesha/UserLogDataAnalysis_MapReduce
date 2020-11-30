package com.highestNoOfTimeLateComing

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object HighestNoOfTimeComingLate {
  def main(args: Array[String]): Unit = {
    val conf = new Configuration()
    val job: Job = new Job(conf, "Late Comers")
    job.setJarByClass(classOf[HighestNoOfTimeComingLateMapper])
    job.setMapperClass(classOf[HighestNoOfTimeComingLateMapper])
    job.setReducerClass(classOf[HighestNoOfTimeComingLateReducer])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path("input"))
    FileOutputFormat.setOutputPath(job, new Path("output_LateComers"))
    job.waitForCompletion(true)
  }
}

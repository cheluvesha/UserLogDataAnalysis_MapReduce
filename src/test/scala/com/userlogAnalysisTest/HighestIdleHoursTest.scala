package com.userlogAnalysisTest

import java.util

import com.highestNoOfIdleHrs.{HighestNoOfIdleHoursMapper, HighestNoOfIdleHoursReducer}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner

import scala.collection.mutable.ListBuffer


@RunWith(classOf[PowerMockRunner])
class HighestIdleHoursTest extends FunSuite with BeforeAndAfter {
  var mapDriver: MapDriver[Object,Text,Text,IntWritable] = _
  var reduceDriver: ReduceDriver[Text,IntWritable,Text,Text] = _
  var mapReduceDriver: MapReduceDriver[Object,Text,Text,IntWritable,Text,Text] = _
  val input1 = "2019-09-18 08:30:01,2,62.72,894.14,50.0,2,266662,0,276411,0.03,0.31,0.25,7896846336,921227264,5305417728,1514033152,646832128,354349056,1315852288,105070592,6553718784,30149586944,19135401984,9459044352,106413,4166,1267769344,321496064,43248,71000,464244,298865,2450592,2549,3840,0,0,0,0,0:08:34.669398,iamnzm@outlook.com,44.0,45.0,python,6"
  val input2 = "2019-09-21 09:20:01,2,650.61,2725.86,60.0,2,1920808,0,2203494,0.8,0.86,0.75,7893979136,4053295104,753819648,4862984192,1271013376,503451648,2583412736,337600512,3185729536,30149586944,25465298944,3129147392,235690,16765,2147739648,656491520,742844,566445,82404,5144480,48254233,39366,62339,0,0,52,0,0:31:54.000042,deepshukla292@gmail.com,0,0,java,65"
  val userDataKey1: String = "iamnzm@outlook.com:2019-09-18: 08:30:01"
  val userDataKey2: String = "deepshukla292@gmail.com:2019-09-21: 09:20:01"
  val userNameWithDate = "iamnzm@outlook.com:2019-09-18"
  val one = 1
  val zero = 0
  val values: util.ArrayList[IntWritable] = new util.ArrayList[IntWritable]()
  values.add(new IntWritable(15))
  values.add(new IntWritable(15))
  val list: ListBuffer[Int] = ListBuffer[Int](1,0,0,0,0,0,1,0,0,0,0,0,0)
  var reducer: HighestNoOfIdleHoursReducer = _
  var mapper: HighestNoOfIdleHoursMapper = _

  def setup(): Unit = {
    mapper = new HighestNoOfIdleHoursMapper
    mapDriver = new MapDriver[Object, Text, Text, IntWritable]()
    mapDriver.setMapper(mapper)

    reducer = new HighestNoOfIdleHoursReducer
    reduceDriver = new ReduceDriver[Text,IntWritable,Text,Text]()
    reduceDriver.setReducer(reducer)

    mapReduceDriver = new MapReduceDriver[Object, Text, Text, IntWritable, Text, Text]()
    mapReduceDriver.setMapper(mapper)
    mapReduceDriver.setReducer(reducer)
  }

  before {
    setup()
  }

  test("givenDataMapperHasToPassValueOneWhenMouseAndKeyboardHitsMoreThanZero") {
    mapDriver.withInput(new LongWritable(),new Text(input1))
    mapDriver.withOutput(new Text(userDataKey1),new IntWritable(one))
    mapDriver.runTest()
  }

  test("givenDataMapperHasToPassValueOneWhenMouseAndKeyboardHitsReturnZero") {
    mapDriver.withInput(new LongWritable(),new Text(input2))
    mapDriver.withOutput(new Text(userDataKey2),new IntWritable(zero))
    mapDriver.runTest()
  }

  test("givenArrayDataMustCheckForZerosAndWhenReturnedMustBeEqual") {
    val result = reducer.checkForZeros(list.toArray)
    assert(result === 6)
  }

  test("givenDataMustEvaluateTimeAndReturnMustEqualToActual") {
    val result = reducer.evaluateTime(userNameWithDate,list)
    assert(("iamnzm@outlook.com",0.5) === result)
  }

}

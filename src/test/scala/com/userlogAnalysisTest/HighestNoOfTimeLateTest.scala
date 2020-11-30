package com.userlogAnalysisTest

import com.highestNoOfTimeLateComing.{HighestNoOfTimeComingLateMapper, HighestNoOfTimeComingLateReducer}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapDriver, MapReduceDriver, ReduceDriver}
import org.junit.runner.RunWith
import org.powermock.modules.junit4.PowerMockRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

/***
 * Test Class for LowestAverageHour
 * class verifies mapper and reduce class methods
 * Dependencies used Junit PowerMocker, Mockito-all-api, MRUnit and Scala Test
 */
@RunWith(classOf[PowerMockRunner])
class HighestNoOfTimeLateTest extends FunSuite with BeforeAndAfter {

  val input = "2019-09-18 09:30:01,2,62.72,894.14,50.0,2,266662,0,276411,0.03,0.31,0.25,7896846336,921227264,5305417728,1514033152,646832128,354349056,1315852288,105070592,6553718784,30149586944,19135401984,9459044352,106413,4166,1267769344,321496064,43248,71000,464244,298865,2450592,2549,3840,0,0,0,0,0:08:34.669398,iamnzm@outlook.com,44.0,45.0,python,6"
  val userNameWithDate = "iamnzm@outlook.com,2019-09-18"
  val userDataKey: String = "iamnzm@outlook.com,2019-09-18,09:30:01"
  val time = "09:30:01"
  var reducer: HighestNoOfTimeComingLateReducer = _
  var mapper: HighestNoOfTimeComingLateMapper = _
  var mapDriver: MapDriver[Object, Text, Text, Text] = _
  var reduceDriver: ReduceDriver[Text,Text,Text,Text] = _
  var mapReduceDriver: MapReduceDriver[Object,Text,Text,Text,Text,Text] = _

  def setup(): Unit = {
    mapper = new HighestNoOfTimeComingLateMapper
    mapDriver = new MapDriver[Object, Text, Text, Text]()
    mapDriver.setMapper(mapper)

    reducer = new HighestNoOfTimeComingLateReducer
    reduceDriver = new ReduceDriver[Text,Text,Text,Text]()
    reduceDriver.setReducer(reducer)

    mapReduceDriver = new MapReduceDriver[Object, Text, Text, Text, Text, Text]()
    mapReduceDriver.setMapper(mapper)
    mapReduceDriver.setReducer(reducer)
  }

  before {
    setup()
  }

  test("givenDataMapperHasToPassValueOneWhenMouseAndKeyboardHitsMoreThanZero") {
    mapDriver.withInput(new LongWritable(),new Text(input))
    mapDriver.withOutput(new Text(userDataKey),new Text(time))
    mapDriver.runTest()
  }

  test("givenDataItMustCheckWhetherItsLateTimeOrEarly") {
    mapReduceDriver.addInput(new LongWritable(), new Text(input))
    mapReduceDriver.addOutput(new Text("iamnzm@outlook.com"), new Text("Late By "+1+" times"))
    mapReduceDriver.runTest()
  }

}

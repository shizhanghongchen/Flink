//package com.bawei.sink
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
//
//case class SensorReading(id: String, timestamp: Long, temperature: Double)
//
//object TestSink {
//  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    val result = env.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\sersor.txt")
//    val r1 = result.map {
//      data => {
//        val strings = data.split(",")
//        SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble).toString
//      }
//    }
////    r1.addSink(new FlinkKafkaProducer011[String]("hadoop102:9092", "testsink", new SimpleStringSchema()))
//    env.execute()
//  }
//}

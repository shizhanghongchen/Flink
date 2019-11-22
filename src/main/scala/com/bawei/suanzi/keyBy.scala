package com.bawei.suanzi

import org.apache.flink.api.common.functions.{FilterFunction, RichFilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object keyBy {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val s1: DataStream[String] = env.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\sersor.txt")
    s1.filter(new MyFilter()).print()
    env.execute()
  }

  class MyFilter() extends RichFilterFunction[String]{
    override def filter(value: String): Boolean = ???
  }
}

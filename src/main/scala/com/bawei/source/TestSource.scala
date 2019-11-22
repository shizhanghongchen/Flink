package com.bawei.source

import java.util.{Properties, Random}

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object TestSource {
  def main(args: Array[String]): Unit = {
    // 1. 获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2. 从集合读取
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_10", 1547718205, 38.101067604893444),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))
    // 2.1 从文件读取数据
    val s2 = env.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\sersor.txt")
    // 2.2 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    // 2.3 topic: String, valueDeserializer: DeserializationSchema[T], props: Properties
//    val s3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    // 2.4 自定义source
    val s4 = env.addSource(new MySensorSource())
    // 3. 打印
    s4.print()
    // 4. 执行
    env.execute()
  }

  class MySensorSource extends SourceFunction[SensorReading]{

    // flag: 表示数据源是否还在正常运行
    var running: Boolean = true

    override def cancel(): Unit = {
      running = false
    }

    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      // 初始化一个随机数发生器
      val rand = new Random()

      var curTemp = 1.to(10).map(
        i => ( "sensor_" + i, 65 + rand.nextGaussian() * 20 )
      )

      while(running){
        // 更新温度值
        curTemp = curTemp.map(
          t => (t._1, t._2 + rand.nextGaussian() )
        )
        // 获取当前时间戳
        val curTime = System.currentTimeMillis()

        curTemp.foreach(
          t => ctx.collect(SensorReading(t._1, curTime, t._2))
        )
        Thread.sleep(100)
      }
    }
  }
}

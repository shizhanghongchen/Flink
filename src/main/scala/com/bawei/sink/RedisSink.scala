package com.bawei.sink


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object RedisSink {
  def main(args: Array[String]): Unit = {
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("192.168.63.102")
      .setPort(6379)
      .build()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\sersor.txt")
    stream.addSink( new RedisSink[String](conf, new MyRedisMapper()) )
    env.execute()
  }
}

class MyRedisMapper() extends RedisMapper[String]{
  // 保存到redis的命令的描述，HSET key field value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temperature" )
  }

  override def getValueFromData(t: String): String = t

  override def getKeyFromData(t: String): String = t
}



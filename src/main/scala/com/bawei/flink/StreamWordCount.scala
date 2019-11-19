package com.bawei.flink

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {

//    val parameterTool = ParameterTool.fromArgs(args)

    // 1. 获取环境
    val streamContext = StreamExecutionEnvironment.getExecutionEnvironment
    val steamFiled = streamContext.socketTextStream("localhost",44444)

    // 2. 计算
    val sf = steamFiled.flatMap(_.split("\\s"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    // 3. 打印
    sf.print().setParallelism(1)

    // 4. 启动
    streamContext.execute()
  }
}

package com.bawei.flink

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    // 1. 环境
    val environment = ExecutionEnvironment.getExecutionEnvironment
    val line = environment.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\qiaoyuan.txt")

    // 2. 计算
    val result = line.flatMap(_.split(" "))
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    // 3. 打印
    result.print()
  }
}

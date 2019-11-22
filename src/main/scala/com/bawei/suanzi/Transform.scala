package com.bawei.suanzi

import org.apache.flink.api.scala._

object Transform {
  def main(args: Array[String]): Unit = {
    // 1. 获取环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2. 读取文件
    val result = env.readTextFile("E:\\workspace_idea\\Flink\\src\\main\\resources\\sersor.txt")
    // 3. 算子操作
    val r1 = result.flatMap(line => line.split(",")).map((_,1)).filter(_._1.contains("sensor_10"))
    r1.print()
  }
}

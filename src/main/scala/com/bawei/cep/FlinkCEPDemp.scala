package com.bawei.cep

import java.util
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.PatternSelectFunction

case class Event(types: String, temp: Double, name: String)
case class Warning(fName: String, message: String)

/**
  * 场景： 获取用户实时登录的信息， 检测出在3秒内重复登录三次失败的用户， 并推送一条告警信息
  */
object FlinkCEPDemp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data: DataStream[Event] = env.fromElements(
      Event("A", 22.2, "test1"),
      Event("B", 22.2, "test2"),
      Event("C", 11.1, "test3"),
      Event("D", 33.3, "2"),
      Event("D", 50.2, "1"),
      Event("D", 35.9, "1")
    )

    // 定义Pattern接口
    val pattern = Pattern.begin[Event]("start") //指定名称
      .where(_.types == "D")
      // 接下来的名称
      .next("middle")
      // 可以将Event事件转换TempEvent事件
      .subtype(classOf[Event])
      .where(_.temp >= 35.0)
      // 结束
      .followedBy("end")
    // 将创建好的Pattern 应用在流上面
    val patternStream = CEP.pattern(data, pattern)

    // 获取触发事件
//    val reslut: DataStream[Option[Iterable[Event]]] = patternStream.select(_.get("end"))
    val reslut = patternStream.select(_.get("middle"))
    reslut.print()
    //    data.print()
    env.execute()
  }
}

package com.bawei.flink

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.types.Row

case class SalesLog(transactionId: String,
                    customer: String,
                    itemId: String,
                    amountPaid: Double)

object WordCountSql {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)
    val filePath = "E:\\workspace_idea\\Flink\\src\\main\\resources\\sale.csv"

    // 1. 拿到dataSet
    val csv: DataSet[SalesLog] = env.readCsvFile[SalesLog](filePath,ignoreFirstLine = true)
    // 2. dataset 转 table
    val salesTable: Table = tableEnv.fromDataSet(csv)
    // 3. 注册表
    tableEnv.registerTable("sales", salesTable)
    // 4. sql
    val resultTable = tableEnv.sqlQuery("select customer, itemId from sales")
    // 5. 打印
    tableEnv.toDataSet[Row](resultTable).print()
  }
}

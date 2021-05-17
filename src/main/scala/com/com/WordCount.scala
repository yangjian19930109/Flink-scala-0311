package com.com

import org.apache.flink.streaming.api.scala._

object WordCount {
	
	def main(args: Array[String]): Unit = {
		
		// 1.获取执行环境
		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		env.setParallelism(1)
		
		// 2.读取端口数据创建流
		val lineDS: DataStream[String] = env.socketTextStream("hadoop106", 9999)
		
		// 3.压平
		val wordDS: DataStream[String] = lineDS.flatMap(_.split(" "))
		
		// 4.转换为元组
		val wordToOneDS: DataStream[(String, Int)] = wordDS.map((_, 1))
		
		// 5.分组
		val keyedStream: KeyedStream[(String, Int), String] = wordToOneDS.keyBy(_._1)
		
		// 6.聚合
		val result: DataStream[(String, Int)] = keyedStream.sum(1)
		
		// 7.打印数据
		result.print()
		
		// 8.执行任务
		env.execute()
	}
	
}

package com.duoduo.scala

import scala.io.Source

object WordCount {
  def main(args: Array[String]): Unit = {

    /**
      * wordcount简写
      *
      * 下划线代表的是第一个参数
      * 下划线只能用一次
      */
    val source = Source.fromFile("D:\\duoduo\\bigdata\\data\\word.txt")  //通过Source读取文件
    source
      .getLines() //获取所有数据
      .flatMap(_.split(",")) //如果数据中存在多个符号，就多扁平化几次
      .map((_, 1)) //每个单词后面标记1
      .toList //转list
      .groupBy(_._1) //通过第一个元素分组
      .map(x => (x._1, x._2.map(_._2).sum)) //将分组后的第二个元素求和
      .map(x => x._1 + "\t" + x._2)
      .foreach(println) //foreach打印最终结果
  }
}
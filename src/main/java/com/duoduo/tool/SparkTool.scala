package com.duoduo.tool

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark工具特征
  */

trait SparkTool {

  var conf: SparkConf = _
  var sc: SparkContext = _

  def main(args: Array[String]): Unit = {
    conf = new SparkConf()
      .setAppName(this.getClass.getName)
    initMaster()
    sc = new SparkContext(conf)
    filePath()
  }

  def initMaster() //指定Spark运行模式

  def filePath() //指定文件路径
}


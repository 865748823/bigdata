package com.duoduo.spark

import com.duoduo.tool.{Content, SparkTool}

/**
  * 使用RDD实现如下功能
  * 1、查询年级排名前十学生各科的分数 [学号,学生姓名，学生班级，科目名，分数]
  * 2、查询总分大于年级平均分的学生 [学号，姓名，班级，总分]
  * 3、查询每科都及格的学生 [学号，姓名，班级，科目，分数]
  * 4、查询偏科最严重的前100名学生  [学号，姓名，班级，科目，分数]
  *
  * #1、查询每个学生所有科目的平均分
  * #2、计算每个学生科目分数和平均分的方差
  * #3、按方差排名，取方差最大的前100名学生
  * #4、管理学生成绩，得到结果
  */
object StudentScoreDemo extends SparkTool{
  override def initMaster(): Unit = {
    conf.setMaster(Content.MASTER)
  }

  override def filePath(): Unit = {
    val studentsRDD = sc
      .textFile(Content.STUDENT_INPUTPATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Student(s(0), s(1), s(2).toInt, s(3), s(4))))

    val scoresRDD = sc
      .textFile(Content.SCORE_INPUTPATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Score(s(0), s(1), s(2).toInt)))

    val courceMap = sc
      .textFile(Content.COURCE_INPUTPATH)
      .map(lines => lines.split(Content.IN_SPLIT))
      .map(s => (s(0), Cource(s(0), s(1), s(2).toInt)))
      .collectAsMap() //collectAsMap函数返回所有元素集合

    var studentScoreInfoRDD = studentsRDD
      .join(scoresRDD)
      .map(x => {
        val stu = x._2._1
        val sco = x._2._2
        StudentScoreInfo(stu.id, stu.name, stu.age, stu.gender, stu.clazz, sco.cource_id, sco.score)
      })

    studentScoreInfoRDD = studentScoreInfoRDD.cache() //多次使用,加入到缓存


    /**
      * 查询年级排名前十学生各科的分数
      * [学号,学生姓名，学生班级，科目名，分数]
      */

    var studentScoreSum = studentScoreInfoRDD
      .map(s => (s.id, s.score))
      .reduceByKey(_ + _) //计算总分

    studentScoreSum=studentScoreSum.cache()

    val topTenStudent = studentScoreSum
      .sortBy(-_._2)  //学生总分降序排列
      .map(_._1)  //取出学生学号
      .take(10) //取前10

    studentScoreInfoRDD
      .filter(s=>topTenStudent.contains(s.id))  //将刚刚取出的10个学号去学生表中筛数据
      .map(s=>{
      //增加科目名称对象
      val courceName= courceMap.get(s.cource_id) match {
        case Some(c)=>c.name
        case None=>"None"
      }
      s.id+Content.OUT_SPLIT+s.name+Content.OUT_SPLIT+s.clazz+Content.OUT_SPLIT+courceName+Content.OUT_SPLIT+s.score
    })//.foreach(println)


    /**
      * 查询总分大于年级平均分的学生
      * [学号，姓名，班级，总分]
      */
    val studentNum = studentScoreSum.count()  //学生总人数
    val studentSum = studentScoreSum.map(_._2).reduce(_+_)  //总分
    val avgSco = studentSum/studentNum //平均总分

    studentScoreSum//总分大于年级平均分的学生
      .filter(x=>x._2>avgSco)
      .join(studentsRDD)
      .map(x=>{
        val stuSum = x._2._1
        val stu = x._2._2
        stu
        stu.id+Content.OUT_SPLIT+stu.name+Content.OUT_SPLIT+stu.clazz+Content.OUT_SPLIT+stuSum
      }).foreach(println)


    /**
      * 查询每科都及格的学生
      * [学号，姓名，班级，科目，分数]
      */

    val courceArr = studentScoreInfoRDD
      .map(s => {
        //获取科目及格分数
        val courceScore = courceMap.get(s.cource_id) match {
          case Some(c) => c.sumScore * 0.6 //及格比例
          case None => 60 //缺省值
        }
        //科目名
        val courceName = courceMap.get(s.cource_id) match {
          case Some(c) => c.name
          case None => "none" //缺省值
        }
        val flag = if (s.score >= courceScore) 1 else 0 //如果成绩大于0.6总分返回1，否则返回0

        (s.id, flag)
      }).reduceByKey(_+_) //将返回值相加，如果等于6则表示都及格
      .filter(_._2==6)
      .map(_._1)
      .collect()

    studentScoreInfoRDD
      .filter(s=>courceArr.contains(s.id))
      .map(s => {
        //增加科目名称列
        val courceName = courceMap.get(s.cource_id) match {
          case Some(c) => c.name
          case None => "no"
        }
        s.id + Content.OUT_SPLIT + s.name + Content.OUT_SPLIT + s.clazz + Content.OUT_SPLIT + courceName + Content.OUT_SPLIT + s.score
      })//.foreach(println)


    /**
      * 查询偏科最严重的前100名学生
      * [学号，姓名，班级，科目，分数]
      */
    val topTendCourceRDD=studentScoreInfoRDD
      .map(s=>{
        (s.id,s.score)
      })
    val stuAvgScoreRDD=topTendCourceRDD
      .reduceByKey(_+_)
      .map(x=>(x._1,x._2/6))  //每个学生的均分

    val top100TendStudentId=topTendCourceRDD
      .join(stuAvgScoreRDD)
      .map(x=>{
        val id = x._1
        val score = x._2._1
        val avgScore = x._2._2
        val squareScore = (score - avgScore)*(score - avgScore)
        (id, squareScore)
      }).reduceByKey(_ + _)//求和
      .sortBy(-_._2)//方差倒序排序
      .take(100)//取出偏科最严重的前100个学生
      .map(_._1)

    studentScoreInfoRDD
      .filter(s=>top100TendStudentId.contains(s.id))
      .map(s => {
        //增加科目名称列
        val courceName = courceMap.get(s.cource_id) match {
          case Some(c) => c.name
          case None => "no"
        }
        s.id + Content.OUT_SPLIT + s.name + Content.OUT_SPLIT + s.clazz + Content.OUT_SPLIT + courceName + Content.OUT_SPLIT + s.score
      })//.foreach(println)
  }
}

/**
  * 样例类
  */

case class Student(id: String, name: String, age: Int, gender: String, clazz: String)

case class Score(student_id: String, cource_id: String, score: Int)

case class Cource(id: String, name: String, sumScore: Int)

case class StudentScoreInfo(id: String, name: String, age: Int, gender: String, clazz: String, cource_id: String, score: Int)

package com.li.syn


import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, Map}

case class AnswerCard(
                       userId: Long,
                       correct: Array[Int],
                       question: Array[Int],
                       point: Array[Int],
                       answerTime: Array[Int],
                       subject: Int,
                       createTime: String
                     )

object MongoSyn2Hive {

  def main(args: Array[String]): Unit = {

    val inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.154:27017,192.168.100.155:27017/huatu_ztk"

    System.setProperty("HADOOP_USER_NAME", "root")
    val updateDate = new SimpleDateFormat("yyyy-MM-dd")

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val conf = new SparkConf()
      .setAppName("MongoSyn2Hive")
//                  .setMaster("local")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.max.dynamic.partitions", "1800")
      .set("mapreduce.map.memory.mb", "10240")
      .set("mapreduce.reduce.memory.mb", "10240")
      .set("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoSamplePartitioner")
      .set("spark.mongodb.input.partitionKey", "_id")
      .set("spark.mongodb.input.partitionSizeMB", "5120")
      .set("spark.mongodb.input.samplesPerPartition", "5000000")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]], classOf[AnswerCard]))

    import com.mongodb.spark.sql._

    val sparkSession = SparkSession
      .builder()
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val sc = sparkSession.sparkContext

    import sparkSession.implicits._
    import sparkSession.sql

    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question"),
          "maxBatchSize" -> "1000000",
          "keep_alive_ms" -> "500")
      )).toDF() // Uses the ReadConfig
    ztk_question.createOrReplaceTempView("ztk_question")
    //    val q2p = sc.broadcast(map.collectAsMap())
    /**
      * mongo 214024
      * spark 205846
      * the mapping of the knowledge to points
      */
    // spark context
    val q2p = sc.broadcast(sparkSession.sql("select _id,points from ztk_question").rdd.filter { r =>
      !r.isNullAt(0) && !r.isNullAt(1) && r.getSeq(1).nonEmpty
    }.map {
      r =>
        val _id: Int = r.get(0).asInstanceOf[Number].intValue()
        val pid = r.getSeq(1).head.asInstanceOf[Double].intValue()
        (_id, pid)
    }.collectAsMap())

    println(q2p.value.size)
    if (q2p.value.isEmpty) {
      sparkSession.stop()
    }

    val ztk_answer_card = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_answer_card"),
          "partitionSizeMB" -> "64",
          "maxBatchSize" -> "100",
          "samplesPerPartition" -> "10000"
        )
      )).toDF() // Uses the ReadConfig
    ztk_answer_card.createOrReplaceTempView("ztk_answer_card2")

    val filterTime = sc.broadcast(args(0).toLong)
    val zac = sql("select userId,corrects,paper.questions,times,createTime,subject from ztk_answer_card2 ")
      .filter {
        r =>
          val r5 = r.get(5)
          r5.getClass.getName match {
            case "java.lang.Integer" =>
              r5.asInstanceOf[Int].longValue() >= filterTime.value
            case "java.lang.Long" =>
              r5.asInstanceOf[Long].longValue() >= filterTime.value
            case _ =>
              false
          }

      }
      .repartition(1000)
      .mapPartitions {

        ite: Iterator[Row] => {

          val arr = new ArrayBuffer[AnswerCard]()
          val format = new SimpleDateFormat("yyyy-MM-dd")
          val q2pMap = q2p.value

          while (ite.hasNext) {

            val r = ite.next()

            val userId = r.get(0).asInstanceOf[Long].longValue()
            val corrects = r.get(1).asInstanceOf[Seq[Int]]
            val questions = r.get(2).asInstanceOf[Seq[Int]]
            val times = r.get(3).asInstanceOf[Seq[Int]]
            val createTime = r.get(4).asInstanceOf[Long].longValue()
            val subject = r.get(5).asInstanceOf[Int].intValue()

            val points = new ArrayBuffer[Int]()
            questions.foreach { qid =>

              val pid: Int = q2pMap.getOrElse(qid, 0)
              points += pid
            }

            arr += AnswerCard(userId, corrects.toArray, questions.toArray, points.toArray, times.toArray, subject, format.format(new Date(createTime)))
          }

          arr.iterator
        }
      }.toDF()

    zac.createOrReplaceTempView("a")
    sql("select userId,correct,question,answerTime,point,subject,createTime from a")
      .repartition(1).write.format("orc").insertInto("ztk_answer_card")
  }
}

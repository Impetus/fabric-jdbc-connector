package com.impetus.fabric.spark.example

import com.impetus.blkch.spark.connector.rdd.ReadConf
import org.apache.spark.sql.fabric.FabricSpark
import FabricSpark.implicits._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object App {
  
  lazy val spark = SparkSession.builder().appName("App").getOrCreate
  
  def main(args: Array[String]): Unit = {
    Class.forName("com.impetus.fabric.jdbc.FabricDriver")
    val readConf = ReadConf(Some(4), None, "Select block_no, previous_hash, transaction_count, channel_id FROM block")
    val df = FabricSpark.load(spark, readConf)
    df.show(false)
  }
}
package com.haiteam

import org.apache.spark.sql.SparkSession

object OrclDataLoad3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    // 접속정보 설정
    var staticUrl = "jdbc:oracle:thin:@192.168.110.16:1522/XE"
    var staticUser = "haiteam"
    var staticPw = "haiteam"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity) 연결
    val selloutDataFromOracle= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성
    println(selloutDataFromOracle.createOrReplaceTempView("selloutTable"))
    println(selloutDataFromOracle.show())



  }



}

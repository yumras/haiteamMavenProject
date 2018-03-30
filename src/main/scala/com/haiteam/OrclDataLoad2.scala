package com.haiteam

import org.apache.spark.sql.SparkSession

object OrclDataLoad2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


    // 파일설정
    var staticUrl = "jdbc:sqlserver://192.168.110.70;databaseName=kopo"
    var staticUser = "haiteam"
      var staticPw = "haiteam"
      var selloutDb = "dbo.KOPO_PRODUCT_VOLUME"

      // jdbc (java database connectivity) 연결
      val selloutDataFromSqlserver= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

      // 메모리 테이블 생성
      selloutDataFromSqlserver.registerTempTable("selloutTable")
      println(selloutDataFromSqlserver.show)
      println(selloutDataFromSqlserver.count)
      }



}

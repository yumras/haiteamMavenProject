package com.haiteam

import org.apache.spark.sql.SparkSession

object MySqlDataLoad {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()


    // 파일설정// 파일설정
    var staticUrl = "jdbc:mysql://192.168.110.112:3306/kopo"
    var staticUser = "root"
    var staticPw = "P@ssw0rd"
    var selloutDb = "KOPO_PRODUCT_VOLUME"

    // jdbc (java database connectivity)
    val selloutDataFromMysql= spark.read.format("jdbc").
      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromMysql.createOrReplaceTempView("selloutTable")
    println(selloutDataFromMysql.show)
    println(selloutDataFromMysql.count)
  }




}

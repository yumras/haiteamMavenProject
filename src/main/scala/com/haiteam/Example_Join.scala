package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_Join {
  val spark = SparkSession.builder().appName("...").config("spark.master", "local").getOrCreate()

  var dataPath = "c:/spark/bin/data/"
  var mainFile = "kopo_channel_seasonality_ex.csv"
  var subFile = "kopo_product_mst.csv"

  // 상대경로 입력
  var mainData = spark.read.format("csv").
    option("header", "true").load(dataPath + mainFile)
  var subData = spark.read.format("csv").
    option("header", "true").load(dataPath + subFile)

  mainData.createOrReplaceTempView("maindata")
  subData.createOrReplaceTempView("subdata")

  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left join subdata b " +
    "on a.productgroup = b.productid")

  var innerData = spark.sql("select a.*, b.productname " +
    "from maindata a inner join subdata b " +
    "on a.productgroup = b.productid")

  leftJoinData.createOrReplaceTempView("leftJoinData")
  innerData.createOrReplaceTempView("innerData")
  leftJoinData.show
  innerData.show


  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var mainFile1 = "kopo_channel_seasonality_new"
  var subFile1 = "kopo_region_mst"

  val mainData1= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> mainFile1,"user" -> staticUser, "password" -> staticPw)).load
  val subData1= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> subFile1,"user" -> staticUser, "password" -> staticPw)).load

  mainData.createOrReplaceTempView("maindata1")
  subData.createOrReplaceTempView("subdata1")


  var leftJoinData1 = spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from maindata1 a left join subdata1 b " +
    "on a.regionid = b.regionid")

  var innerData1 = spark.sql("select a.regionid, b.regionname, a.product, a.yearweek, a.qty " +
    "from maindata1 a inner join subdata1 b " +
    "on a.regionid = b.regionid")

  leftJoinData1.createOrReplaceTempView("leftJoinData")
  leftJoinData1.show

  innerData1.createOrReplaceTempView("innerData")
  innerData1.show


}

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

  mainData.createTempView("maindata")
  subData.createTempView("subdata")

  var leftJoinData = spark.sql("select a.*, b.productname " +
    "from maindata a left outer join subdata b " +
    "on a.productgroup = b.productid")

  var innerData = spark.sql("select a.*, b.productname " +
    "from maindata a inner join subdata b " +
    "on a.productgroup = b.productid")

  leftJoinData.createTempView("leftJoinData")
  innerData.createTempView("innerData")

}

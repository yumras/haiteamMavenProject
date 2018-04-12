package com.haiteam

import org.apache.spark.sql.SparkSession

object OrclDataJoin1_0412 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

    //oracle connection
    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var masterDb = "kopo_product_mst"

    val selloutDf = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    val mstDf = spark.read.format("jdbc").option("encoding", "UTF-8").
      options(Map("url" -> staticUrl, "dbtable" -> masterDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")

    mstDf.createOrReplaceTempView("mstTable")

    var resultDf = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as KEYCOL," +
      "a.regionid AS ACCOUNTID, " +
      "a.product AS PRODUCT, " +
      "a.yearweek AS YEARWEEK, " +
      "cast(qty as double) AS QTY, " +
      "b.product_name AS PRODUCT_NAME " +
      "from selloutTable A " +
      "left join mstTable B " +
      "on a.product = b.product_id")

    var resultDfColumns = resultDf.columns.map(x=>{ x.toLowerCase() })

    var keyNo = resultDfColumns.indexOf("keycol")
    var accountidNo = resultDfColumns.indexOf("accountid")
    var productNo = resultDfColumns.indexOf("product")
    var yearweekNo = resultDfColumns.indexOf("yearweek")
    var qtyNo = resultDfColumns.indexOf("qty")
    var product_nameNo = resultDfColumns.indexOf("product_name")


  }
}

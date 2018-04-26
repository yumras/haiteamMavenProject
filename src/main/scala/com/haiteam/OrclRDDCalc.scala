package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object OrclRDDCalc {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()

    // oracle connection
    val staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "kopo_channel_seasonality_new"
    var productNameDb = "kopo_product_mst"

    var selloutDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    var productMasterDf = spark.read.format("jdbc").
      options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
        "user" -> staticUser,
        "password" -> staticPw)).load

    selloutDf.createOrReplaceTempView("selloutTable")
    productMasterDf.createOrReplaceTempView("mstTable")

    // Join 해서 rawData 생성
    var rawData = spark.sql("select " +
      "concat(a.regionid,'_',a.product) as keycol, " +
      "a.regionid as accountid, " +
      "a.product, " +
      "a.yearweek, " +
      "cast(a.qty as double) as qty, " +
      "b.product_name as productname " +
      "from selloutTable a " +
      "left join mstTable b " +
      "on a.product = b.product_id")


    rawData.show(2)

    // 컬럼 번호 붙이기
    var rawDataColumns = rawData.columns
    var keyNo = rawDataColumns.indexOf("keycol")
    var accountidNo = rawDataColumns.indexOf("accountid")
    var productNo = rawDataColumns.indexOf("product")
    var yearweekNo = rawDataColumns.indexOf("yearweek")
    var qtyNo = rawDataColumns.indexOf("qty")
    var productnameNo = rawDataColumns.indexOf("productname")

    // rawData을 RDD로 변환
    var rawRdd = rawData.rdd


    // rawData filtering
    var filteredRdd = rawRdd.filter(x=>{
      var checkValid = false
      if((x.getString(yearweekNo).substring(0,4).toInt >= 2016) &&
        (x.getString(yearweekNo).substring(4).toInt != 53) &&
        (x.getString(productNo) == "PRODUCT1") ||
        (x.getString(productNo) == "PRODUCT2")) {
        checkValid = true;
      }
      checkValid
    })


    // import org.apache.spark.sql.Row 추가
    // RDD 가공연산(Row 형태는 컬럼 접근시 x.getString(indexNo))
    var mapRdd = rawRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      Row( x.getString(keyNo),
        x.getString(yearweekNo),
        qty, x.getString(qtyNo)
      )})

    // RDD 가공연산(Row 형태는 컬럼 접근시 x._1,x._2,... )
    var mapRdd2 = rawRdd.map(x=>{
      var qty = x.getString(qtyNo).toDouble
      var maxValue = 700000
      if(qty > 700000){qty = 700000}
      ( x.getString(keyNo),
        x.getString(yearweekNo),
        qty, x.getString(qtyNo)
      )})



    var mapexRdd = rawRdd

    var x = mapexRdd.first

    var y = mapexRdd.filter(x=> {
      x.getDouble(qtyNo) > 700000
    }).first

    // 처리로직 : 거래량이 MAXVALUE 이상인 건은 MAXVALUE로 치환한다.

    var MAXVALUE = 700000

    var org_qty = y.getDouble(qtyNo)

    var new_qty = org_qty

    new_qty > MAXVALUE

    new_qty = MAXVALUE

    if(new_qty > MAXVALUE) {
      new_qty = MAXVALUE
    }

    var z = mapexRdd.filter(row=>{ row.getDouble(qtyNo) > 700000 }).first



  }
}

package com.haiteam

import org.apache.spark.sql.SparkSession

object OrclDataJoinRDD_0419 {
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

    // 설정 부적합 로직 정제
    var rawExRdd = rawRdd.filter(x=>{
      var checkValid = true
      // 설정 부적합 로직
      if (x.getString(yearweekNo).length !=6){
        checkValid = false;
      }
      checkValid
    })

    //실습1 (연주차가 52주차 이상인 값 제거)
    var filterexRdd = rawRdd.filter(x=>{
      var checkValid = true
      if (x.getString(yearweekNo).substring(4).toInt > 52) {
        checkValid = false;
      }
      checkValid
    })
    var x =filterexRdd.first

    //    // checkValid 제거하고 한 버전(빠르지만 가독성 떨어짐)
    //    var filterex2Rdd = rawRdd.filter(x=>{
    //      (x.getString(yearweekNo).substring(4).toInt <= 52)
    //    })
    //    var x =filterex2Rdd.first

    //실습2 (상품정보가 PRODUCT1,2인 정보만 필터링) - (추천 방법)
    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT1", "PRODUCT2")
    // 세트 타입으로 변환
    var productSet = productArray.toSet

    var resultRdd = rawRdd.filter(x=>{
      var checkValid = true
      // 데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)
      if(productSet.contains(productInfo)){
        checkValid = true
      }
      checkValid
    })

    //실습2 (상품정보가 PRODUCT1,2인 정보만 필터링)
    // - (비추천 방법: if 여러개 돌리게 되면 비효율적)
    var filterex2Rdd = rawRdd.filter(x=>{
      var checkValid = false
      if((x.getString(productNo) == "PRODUCT1") ||
        (x.getString(productNo) == "PRODUCT2")) {
        checkValid = true;
      }
      checkValid
    })

    var ans =filterex2Rdd.first



    // RDD 값 화면 출력
    filterex2Rdd.take(3).foreach(println)
    filterex2Rdd.collect.toArray.foreach(println)






  }
}

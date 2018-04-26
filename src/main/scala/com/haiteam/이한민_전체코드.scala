package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import com.midterm.getWeekInfo.getWeekInfo

object 이한민_전체코드 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.master","local").getOrCreate()


    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"   //<- 접속아이디 보고 바꾸기
    var staticPw = "kopo"     //<- 비밀번호 보고 바꾸기
    var File = "kopo_channel_seasonality_new"   //<- 파일명 보고 바꾸기
    var dada1 = spark.read
    val Data1 = spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> File,"user" -> staticUser, "password" -> staticPw)).load
    Data1.createOrReplaceTempView("dataTb")

    // 1번답: 데이터 불러오고 확인 //////////////////////////////////////////////////
    Data1.show(2)

    // 2번답: SQL분석 /////////////////////////////////////////////////////////////
    var newData1 = spark.sql("select regionid as REGIONID, product AS PRODUCT, yearweek AS YEARWEEK, cast(qty as double) as QTY, cast(qty*1.2 as double) as QTY_NEW from dataTb")

    //데이터 칼럼 번호 매기기
    var newDataColumns = newData1.columns
    var regionNo = newDataColumns.indexOf("REGIONID")
    var productNo = newDataColumns.indexOf("PRODUCT")
    var yearweekNo = newDataColumns.indexOf("YEARWEEK")
    var qtyNo = newDataColumns.indexOf("QTY")
    var qty_newNo = newDataColumns.indexOf("QTY_NEW")

    // newData을 RDD로 변환
    var newRdd = newData1.rdd

    // 분석대상 제품군 등록
    var productArray = Array("PRODUCT1", "PRODUCT2")

    // 세트 타입으로 변환
    var productSet = productArray.toSet

    // VALIDYEAR, VALIDWEEK 선언
    var VALIDYEAR = 2016
    var VALIDWEEK = 52

    // 4번답: 정제 //////////////////////////////////////////////////////////////////
    var filteredRdd = newRdd.filter(x=>{
      var checkValid = false
      // 데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
      var productInfo = x.getString(productNo)
      var yearInfo = x.getString(yearweekNo).substring(0,4)toInt
      var weekInfo = getWeekInfo(x.getString(yearweekNo))

      if((productSet.contains(productInfo))&&
        (weekInfo < VALIDWEEK) &&
        (yearInfo >= VALIDYEAR)) {
        checkValid = true
      }
      checkValid
    })

    // RDD 값 화면 출력
    filteredRdd.take(3).foreach(println)
    filteredRdd.collect.toArray.foreach(println)

    // Rdd를 DataFrame으로 저장하기
    val finalResultDf = spark.createDataFrame(filteredRdd,
      StructType(
        Seq(
          StructField("regionid", StringType),
          StructField("product", StringType),
          StructField("yearweek", StringType),
          StructField("qty", DoubleType),
          StructField("qty_new", DoubleType))))

    // 5번답: 데이터 저장////////////////////////////////////////////////////////////
    // 데이터 저장(postgres)
    var myUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"
    val prop = new java.util.Properties
    prop.setProperty("driver", "org.postgresql.Driver")
    prop.setProperty("user", "kopo")
    prop.setProperty("password", "kopo")
    val table = "KOPO_ST_RESULT_HML"
    // append
    finalResultDf.write.mode("overwrite").jdbc(myUrl, table, prop)


  }
}

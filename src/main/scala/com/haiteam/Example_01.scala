package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_01 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()
      //load data


    //배열 생성
    var test2Array = Array(22, 33, 50, 70, 90, 100)
    //filter함수 사용
//    var answer2 = test2Array.filter(x=>{x%10 == 0});

    var answer2 = test2Array.filter(x=>{
      var data = x.toString             // String 변환
      var dataSize = data.size          // String 갯수

      var lastChar = data.substring(dataSize-1).toString    // 마지막자리 숫자 추출

      lastChar.equalsIgnoreCase("0")    // 0인 경우만 남김?
    })

    var arraySize = answer2.size
    for (i<-0 until arraySize){
      println(answer2(i))
    }
  }
}

package com.haiteam

import org.apache.spark.sql.SparkSession

object Array2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("hkProject").
      config("spark.master", "local").
      getOrCreate()

//      var score = 9
//      var level = "기타"
//      if(score>9) {
//        level = "수"
//      }else if((score > 8) &&
//               (score <=9)){
//        level = "우"
//      }else{
//        level = "양"
//      }
//
//      println(level)

    var priceData = Array(1000.0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size
    var i = 0

//     while(i < priceDataSize){
//       var promotionEffect = priceData(i) * promotionRate
//       priceData(i) = priceData(i) - promotionEffect
//       i=i+1
//      }

    priceData.map(x=>{
      var promotionEffect = x*0.2
      x-promotionEffect
    })


  }

}

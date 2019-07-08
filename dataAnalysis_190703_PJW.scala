package com.pjw

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.expressions.Window

object dataAnalysis_190703_PJW {
  def main(args: Array[String]): Unit = {
    var csvFile = "refined_join_data.csv"

    var refined_join_data =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + csvFile)

    // 1. collectAsMap으로 yearweek기준 qty 추출
    var refined_join_data_rdd = refined_join_data.rdd

    var refined_columns = refined_join_data.columns
    var refined_regionSeg1No = refined_columns.indexOf("regionseg")
    var refined_productSeg1No = refined_columns.indexOf("productseg1")
    var refined_productgroupNo = refined_columns.indexOf("productgroup")
    var refined_salesidNo = refined_columns.indexOf("salesid")
    var refined_regionSeg3No = refined_columns.indexOf("regionseg3")
    var refined_itemNo = refined_columns.indexOf("item")
    var refined_yearweekNo = refined_columns.indexOf("yearweek")
    var refined_map_priceNo = refined_columns.indexOf("map_price")
    var refined_irNo = refined_columns.indexOf("ir")
    var refined_pmapNo = refined_columns.indexOf("pmap")
    var refined_pmap10No = refined_columns.indexOf("pmap10")
    var refined_pro_percentNo = refined_columns.indexOf("pro_percent")
    var refined_qtyNo = refined_columns.indexOf("qty")
    var refined_promotionYN = refined_columns.indexOf("promotionNY")

    var searchQtyrdd = refined_join_data_rdd.groupBy( x => { (x.getString(refined_regionSeg1No), x.getString(refined_productSeg1No), x.getString(refined_productgroupNo)
                                                              , x.getString(refined_salesidNo), x.getString(refined_regionSeg3No)
                                                              , x.getString(refined_itemNo), x.getString(refined_yearweekNo) )
    }).map( x => {
      var key = x._1
      var data = x._2

      var qty = data.map (x => {
        (key,x.getString(refined_qtyNo))
      })
      qty
    }).collectAsMap

    //boealean
    searchQtyrdd.contains("A01",null,"PG05","SALESID0001","SITEID0003","ITEM0073","201539")

    //값 추출
    searchQtyrdd("A01",null,"PG05","SALESID0001","SITEID0003","ITEM0073","201539")

    // getWeek 사용
    def getWeek(inputYearWeek: String, gapWeek: Int): String = {
      var currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt
      var totalWeek = currWeek + gapWeek
      var result = ""

      var calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")

      calendar.setTime(dateFormat.parse(currYear + "1231"))
      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if( gapWeek > 0){
        while(maxWeek < totalWeek){
          currYear = currYear + 1
          totalWeek = totalWeek - maxWeek
          calendar.setTime(dateFormat.parse(currYear + "1231"))
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
        }
      }
      else {
        while (totalWeek <= 0){
          currYear = currYear - 1
          totalWeek = totalWeek + maxWeek
          calendar.setTime(dateFormat.parse(currYear + "1231"))
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
        }
      }

      return (currYear).toString() + "%02d".format((totalWeek))
    }

    //이동평균 매핑
    var mavg_data_rdd = refined_join_data_rdd.groupBy( x => { (x.getString(refined_regionSeg1No), x.getString(refined_regionSeg3No), x.getString(refined_productgroupNo))
    }).flatMap( x => {
      var key = x._1
      var data = x._2

      //(x.getString(refined_regionSeg1No), x.getString(refined_productSeg1No), x.getString(refined_productgroupNo), x.getString(refined_salesidNo), x.getString(refined_regionSeg3No), x.getString(refined_itemNo)
      var key1 = key._1
      var key2 = key._2
      var key3 = key._3

      // 1. 이동평균범위 설정, sum, size 선언
      var avgRange = 5
      var minRange = -(avgRange-1)/2
      var maxRange = (avgRange-1)/2

      // 2. 이동평균의범위 만큼 getWeek로 contains로 true, false 체크후 true면 qty값을 가져온다

      var result = data.map( x => {

        var movingAvg = 0.0d
        var seasonality = 0.0d
        var movingSum = 0.0d
        var movingSize = 0.0d
        var currentYearweek = x.getString(refined_yearweekNo)
        var currentQty = x.getString(refined_qtyNo).toDouble

        for(i <- minRange to maxRange) {
          if (searchQtyrdd.contains(key1, null, key3, x.getString(refined_salesidNo), key2, x.getString(refined_itemNo), getWeek(currentYearweek,i))){
            movingSum = movingSum + searchQtyrdd(key1, null, key3, x.getString(refined_salesidNo), key2, x.getString(refined_itemNo), getWeek(currentYearweek,i)).toInt
            movingSize = movingSize + 1
          }
        }

        if(movingSize != 0) {
          movingAvg = movingSum / movingSize
        }

        (key1,
          x.getString(refined_productSeg1No),
          key3,
          x.getString(refined_salesidNo),
          key2,
          x.getString(refined_itemNo),
          x.getString(refined_yearweekNo),
          x.getString(refined_map_priceNo),
          x.getString(refined_irNo),
          x.getString(refined_pmapNo),
          x.getString(refined_pmap10No),
          x.getString(refined_pro_percentNo),
          x.getString(refined_qtyNo),
          x.getString(refined_promotionYN),
          movingAvg.toString
        )
      })
      result
    })

    var movingAvgDF = mavg_data_rdd.toDF("regionseg", "productseg1","productgroup","salesid","regionseg3","item"
      ,"yearweek","map_price","ir","pmap","pmap10","pro_percent","qty","promotionNY","moving_avg")

    var movingAvg_data_rdd = movingAvgDF.rdd

    //이동평균 컬럼 인덱스 추가
    var movingAvg_columns = movingAvgDF.columns
    var movingAvgNo = movingAvg_columns.indexOf("moving_avg")

    // 시즈널리티 계산
    var seasonality_data_rdd = movingAvg_data_rdd.groupBy(x => {(x.getString(refined_regionSeg1No), x.getString(refined_regionSeg3No), x.getString(refined_productgroupNo) )
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3

      var result = data.map(x => {
        var seasonality = 0.0d
        var currentQty = x.getString(refined_qtyNo).toInt
        var movingAvg = x.getString(movingAvgNo).toDouble

        if(movingAvg != 0) {
          seasonality = currentQty / movingAvg
        }

        (key1,
          x.getString(refined_productSeg1No),
          key3,
          x.getString(refined_salesidNo),
          key2,
          x.getString(refined_itemNo),
          x.getString(refined_yearweekNo),
          x.getString(refined_map_priceNo),
          x.getString(refined_irNo),
          x.getString(refined_pmapNo),
          x.getString(refined_pmap10No),
          x.getString(refined_pro_percentNo),
          x.getString(refined_qtyNo),
          x.getString(refined_promotionYN),
          x.getString(movingAvgNo),
          seasonality
        )
      })
      result
    })

    var refinedDF = seasonality_data_rdd.toDF("regionseg", "productseg1","productgroup","salesid","regionseg3","item"
      ,"yearweek","map_price","ir","pmap","pmap10","pro_percent","qty","promotionNY","moving_avg", "seasonality")

    refinedDF.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/seasonality_data.csv")
  }
}

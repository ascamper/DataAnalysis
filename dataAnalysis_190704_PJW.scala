package com.pjw

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import spark.implicits._

object dataAnalysis_190704_PJW {
  def main(args: Array[String]): Unit = {
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

      if (gapWeek > 0) {
        while (maxWeek < totalWeek) {
          currYear = currYear + 1
          totalWeek = totalWeek - maxWeek
          calendar.setTime(dateFormat.parse(currYear + "1231"))
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
        }
      }
      else {
        while (totalWeek <= 0) {
          currYear = currYear - 1
          totalWeek = totalWeek + maxWeek
          calendar.setTime(dateFormat.parse(currYear + "1231"))
          maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
        }
      }

      return (currYear).toString() + "%02d".format((totalWeek))
    }

    var csvFile = "seosonality_data.csv"

    var seasonality_data =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + csvFile)

    var seasonality_data_rdd = seasonality_data.rdd

    var columns = seasonality_data.columns
    var regionSeg1No = columns.indexOf("regionseg")
    var productSeg1No = columns.indexOf("productseg1")
    var productgroupNo = columns.indexOf("productgroup")
    var salesidNo = columns.indexOf("salesid")
    var regionSeg3No = columns.indexOf("regionseg3")
    var itemNo = columns.indexOf("item")
    var yearweekNo = columns.indexOf("yearweek")
    var map_priceNo = columns.indexOf("map_price")
    var irNo = columns.indexOf("ir")
    var pmapNo = columns.indexOf("pmap")
    var pmap10No = columns.indexOf("pmap10")
    var pro_percentNo = columns.indexOf("pro_percent")
    var qtyNo = columns.indexOf("qty")
    var promotionYN = columns.indexOf("promotionNY")
    var moving_avgNo = columns.indexOf("moving_avg")
    var seasonalityNo = columns.indexOf("seasonality")
    // 1. 전체에서 가장 최신주차를 전역변수로 유지

    var maxYearWeek = seasonality_data_rdd.map(x => {
      x.getString(yearweekNo)
    }).max

    //qty 추출
    var searchQtyrdd = seasonality_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(productSeg1No), x.getString(productgroupNo), x.getString(salesidNo), x.getString(regionSeg3No)
        , x.getString(itemNo), x.getString(yearweekNo))
    }).map(x => {
      var key = x._1
      var data = x._2

      var qty = data.map(x => {
        x.getString(qtyNo)
      })
      (key, qty.head)
    }).collectAsMap

    //seasonality 추출
    var searchSeasonalityrdd = seasonality_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(productSeg1No), x.getString(productgroupNo), x.getString(salesidNo), x.getString(regionSeg3No)
        , x.getString(itemNo), x.getString(yearweekNo))
    }).map(x => {
      var key = x._1
      var data = x._2

      var seasonality = data.map(x => {
        x.getString(seasonalityNo)
      })
      (key, seasonality.head)
    }).collectAsMap

    //최종 데이터 정제
    var filtered_seasonality_data_rdd = seasonality_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(productSeg1No), x.getString(productgroupNo)
        , x.getString(salesidNo), x.getString(regionSeg3No)
        , x.getString(itemNo))
    }).filter(x => {
      var checkValid = true

      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3
      var key4 = key._4
      var key5 = key._5
      var key6 = key._6

      var groupMaxYearweek = data.map(x => {
        x.getString(yearweekNo)
      }).max

      if (groupMaxYearweek < getWeek(maxYearWeek, -7)) {
        checkValid = false
      }
      else {
        // 201620 부터 201627 까지의 qty가 모두 0 인 경우를 걸러낸다.
        // 1. contains로 존재여부를 판단.
        // 2. 존재하면 qty가 0인지를 판단
        // 3. 존재하지 않으면 0으로 판단.
        // 4. 0일때 카운트를 올린다.
        // 5. count가 8이면 checkValid = false
      }
      checkValid
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3
      var key4 = key._4
      var key5 = key._5
      var key6 = key._6

      // 1. 예측기간 fcst_window = 5
      var fcst_window = 5
      // 2. 그룹별 maxYearweek 부터 + fcst_window 까지 row 생성
      var predictionWeekArray = Array(getWeek(maxYearWeek, 1))

      for (i <- 2 to fcst_window) {
        predictionWeekArray ++= Array(getWeek(maxYearWeek, i))
      }

      var predictionWeekMap = predictionWeekArray.map(x => {
        Row(key._1, key._2, key._3, key._4, key._5, key._6, x, "0", "0", "0", "0", "0", "0", "N", "0", "0")
      })

      var origin = data.map(x => {
        Row(key1,
          key2,
          key3,
          key4,
          key5,
          key6,
          x.getString(yearweekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.getString(promotionYN),
          x.getString(moving_avgNo),
          x.getString(seasonalityNo)
        )
      })

      // 예측주차의 컬럼과 기존 데이터와 합치기
      var result = predictionWeekMap ++ origin

      result
    })

    //예측주차의 fcst, seasonality, time_series 산출
    var final_data_rdd = filtered_seasonality_data_rdd.groupBy(x => {(x.getString(regionSeg1No), x.getString(productSeg1No), x.getString(productgroupNo), x.getString(salesidNo), x.getString(regionSeg3No) , x.getString(itemNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3
      var key4 = key._4
      var key5 = key._5
      var key6 = key._6

      var yearweekArray = data.map( x => x.getString(yearweekNo)).toArray
      var groupMinYearweek = yearweekArray.min
      var groupMinYear = groupMinYearweek.substring(0,4)
      var groupMaxYearweek = yearweekArray.max
      var groupMaxYear = groupMaxYearweek.substring(0,4)

      var groupCorrentYearweek = data.map(x =>{ x.getString(yearweekNo)}).head
      var groupCorrentWeek = groupCorrentYearweek.substring(4)

      var result = data.map( x => {
        var fcst = 0.0d
        var fcst_seasonality = 0.0d
        // 2. maxYearweek 부터 -4 까지의 평균 qty를 fcst에 넣어준다.
        var avg_4_sum = 0.0d
        var avg_4_seasonality = 0.0d
        var avg_4_size = 4
        var week_seasonality = 0.0d
        var week_seasonality_size = 0
        var time_series = 0.0d

        if( x.getString(yearweekNo) > maxYearWeek) {
          // 1. fcst 컬럼 추가.
          for (i <- -3 to 0) {
            if (searchQtyrdd.contains(key1, null, key3, key4, key5, key6, getWeek(maxYearWeek, i))) {
              avg_4_sum = avg_4_sum + searchQtyrdd(key1, null, key3, key4, key5, key6, getWeek(maxYearWeek, i)).toDouble
              avg_4_seasonality = avg_4_seasonality + searchSeasonalityrdd(key1, null, key3, key4, key5, key6, getWeek(maxYearWeek, i)).toDouble
            }
          }
          fcst = avg_4_sum / avg_4_size
          fcst_seasonality = avg_4_seasonality / avg_4_size
          // 3. 해당 주차의 평균 계절성지수를 구해주는 함수를 돌려 얻은 값을 seasonality에 넣어준다.
          for (i <- groupMinYear.toInt to groupMaxYear.toInt) {
            var tempYearweek = i.toString + groupCorrentWeek

            if (searchSeasonalityrdd.contains(key1, null, key3, key4, key5, key6, tempYearweek)) {
              week_seasonality = week_seasonality + searchSeasonalityrdd(key1, null, key3, key4, key5, key6, tempYearweek).toDouble
              week_seasonality_size = week_seasonality_size + 1
            }

            if (week_seasonality_size != 0) {
              week_seasonality = week_seasonality / week_seasonality_size
            }
          }
          // 4. time_series 컬럼 추가.
          if (avg_4_seasonality != 0) {
            time_series = (fcst * week_seasonality) / avg_4_seasonality
          }
        }
        // 5. 2 * 3 / (maxYearweek부터 -4 까지의 평균 seasonality) 의 값을 time_series에 넣어준다.
        (key1,
          key2,
          key3,
          key4,
          key5,
          key6,
          x.getString(yearweekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.getString(promotionYN),
          x.getString(moving_avgNo),
          avg_4_seasonality,
          fcst,
          time_series)
      })
      result
    })

    var filteredDF = final_data_rdd.toDF("regionseg", "productseg1","productgroup","salesid","regionseg3","item"
      ,"yearweek","map_price","ir","pmap","pmap10","pro_percent","qty","promotionNY","moving_avg", "seasonality", "fcst", "time_series")

    filteredDF.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/final_data.csv")

    // 3. 전체 데이터에서 arrary목록에 있는 데이터를 제거
    // 4. 그룹별 예측기간에 해당하는 row를 삽입
    // 5. 삽입하는 과정에서 예측에 사용된 주차범위의 qty평균을 fcst로 새로 컬럼을 뽑아내면서 삽입

  }

}

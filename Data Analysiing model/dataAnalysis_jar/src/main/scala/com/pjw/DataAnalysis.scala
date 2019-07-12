package com.pjw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    // 1.getWeek 정의
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

      return currYear.toString() + "%02d".format(totalWeek)
    }

    // parameterTable 로드
    var AVERAGE_WINDOW = 5
    var AVERAGE_WINDOW_KEY = "average_window"
    var PREDICT_RANGE = 5
    var PREDICT_RANGE_KEY = "predict_range"
    var BASE_DATA = 4
    var BASE_DATA_KEY = "base_data"
    var MAXWEEK = 201627
    var MAXWEEK_KEY = "maxyearweek"

    var parameterUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var parameterUser = "kopo"
    var parameterPw = "kopo"
    var parameterDb = "parameter_group4"

    val tempTableDF = spark.read.format("jdbc").options(Map("url" -> parameterUrl, "dbtable" -> parameterDb,
      "user" -> parameterUser, "password" -> parameterPw)).load

    var tempTableDF_rdd = tempTableDF.rdd

    var parameterTableDF_rdd = tempTableDF_rdd.map(x => {
      Row(x.getString(0), x.get(1).toString.toDouble.toInt)
    })

    // 각 파라미터를 전역변수에 매칭시킨다.------------
    var searchParameterValue = parameterTableDF_rdd.groupBy(x => {
      x.getString(0)
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var result = data.map(x => {
        (key, x.getInt(1))
      })
      result
    }).collectAsMap

    // contains로 해당 이름의 parameter가 존재하는지 여부 판단. 존재한다면 값을 가져오고 아니면 초기값으로 진행------------
    if (searchParameterValue.contains(AVERAGE_WINDOW_KEY)) {
      AVERAGE_WINDOW = searchParameterValue(AVERAGE_WINDOW_KEY)
    }
    if (searchParameterValue.contains(PREDICT_RANGE_KEY)) {
      PREDICT_RANGE = searchParameterValue(PREDICT_RANGE_KEY)
    }
    if (searchParameterValue.contains(BASE_DATA_KEY)) {
      BASE_DATA = searchParameterValue(BASE_DATA_KEY) - 1
    }
    if (searchParameterValue.contains(MAXWEEK_KEY)) {
      MAXWEEK = searchParameterValue(MAXWEEK_KEY).toInt
    }

    // 1. db에서 table 정보를 불러온다.
    var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "common_group4"

    // 2. 불러온 table 정보를 데이터프레임화 한다.
    val selloutDataFromOracle = spark.read.format("jdbc").options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
      "user" -> staticUser, "password" -> staticPw)).load

    // 3. 데이터프레임을 rdd로 변환한다.
    var final_data_rdd = selloutDataFromOracle.rdd

    //인덱스 설정
    var columns = selloutDataFromOracle.columns
    var regionSeg1No = columns.indexOf("regionseg".toUpperCase)
    var regionSeg3No = columns.indexOf("regionseg3".toUpperCase)
    var salesidNo = columns.indexOf("salesid".toUpperCase)
    var productgroupNo = columns.indexOf("productgroup".toUpperCase)
    var itemNo = columns.indexOf("item".toUpperCase)
    var yearweekNo = columns.indexOf("yearweek".toUpperCase)
    var yearNo = columns.indexOf("year".toUpperCase)
    var weekNo = columns.indexOf("week".toUpperCase)
    var map_priceNo = columns.indexOf("map_price".toUpperCase)
    var irNo = columns.indexOf("ir".toUpperCase)
    var pmapNo = columns.indexOf("pmap".toUpperCase)
    var pmap10No = columns.indexOf("pmap10".toUpperCase)
    var pro_percentNo = columns.indexOf("pro_percent".toUpperCase)
    var qtyNo = columns.indexOf("qty".toUpperCase)
    var promotionYN = columns.indexOf("promotionNY".toUpperCase)
    var moving_avgNo = columns.indexOf("moving_avg".toUpperCase)
    var seasonalityNo = columns.indexOf("seasonality".toUpperCase)
    var fcstNo = columns.indexOf("fcst".toUpperCase)
    var time_seriesNo = columns.indexOf("time_series".toUpperCase)

    // maxweek 정의
    var maxYearWeek = MAXWEEK.toString()

    // qty 추출 정의
    var searchQtyrdd = final_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No), x.getString(salesidNo),
        x.getString(productgroupNo), x.getString(itemNo), x.getString(yearweekNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var qty = data.map(x => {
        (key, x.getString(qtyNo))
      })
      qty
    }).collectAsMap

    //seasonality 추출 정의
    var searchSeasonalityrdd = final_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No),
        x.getString(salesidNo), x.getString(productgroupNo), x.getString(itemNo), x.getString(yearweekNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var seasonality = data.map(x => {
        (key, x.getString(seasonalityNo))
      })
      seasonality
    }).collectAsMap

    //기존 예측주차를 걷어내고 새로 설정된 파라미터로 예측주차 추가 ---------------------------------
    var step1_final_data_rdd = final_data_rdd.filter(x => {
      x.getString(yearweekNo) <= maxYearWeek
    })

    var step2_final_data_rdd = step1_final_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No), x.getString(salesidNo)
        , x.getString(productgroupNo), x.getString(itemNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3
      var key4 = key._4
      var key5 = key._5

      var fcst_window = PREDICT_RANGE

      var predictionWeekArray = Array(getWeek(maxYearWeek, 1))

      for (i <- 2 to fcst_window) {
        predictionWeekArray ++= Array(getWeek(maxYearWeek, i))
      }

      var predictionWeekMap = predictionWeekArray.map(x => {
        Row(key._1, key._2, key._3, key._4, key._5, x, x.substring(0, 4), x.substring(4), "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")
      })

      /////////////////////////////////////////////////////////////////////////////////////////////////190709
      var origin = data.map(x => {
        Row(x.getString(regionSeg1No),
          x.getString(regionSeg3No),
          x.getString(salesidNo),
          x.getString(productgroupNo),
          x.getString(itemNo),
          x.getString(yearweekNo),
          x.getString(yearNo),
          x.getString(weekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.get(promotionYN).toString,
          x.getString(moving_avgNo),
          x.getString(seasonalityNo),
          x.getString(fcstNo),
          x.getString(time_seriesNo)
        )
      })

      // 예측주차의 컬럼과 기존 데이터와 합치기
      var result = predictionWeekMap ++ origin

      result
    })

    var mavg_data_rdd = step2_final_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No), x.getString(productgroupNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3

      // 1. 이동평균범위 설정, sum, size 선언
      var avgRange = AVERAGE_WINDOW
      var minRange = -(avgRange - 1) / 2
      var maxRange = (avgRange - 1) / 2

      // 2. 이동평균의범위 만큼 getWeek로 contains로 true, false 체크후 true면 qty값을 가져온다
      var result = data.map(x => {

        var movingAvg = 0.0d
        var seasonality = 0.0d
        var movingSum = 0.0d
        var movingSize = 0.0d
        var currentYearweek = x.getString(yearweekNo)
        var currentQty = x.getString(qtyNo).toDouble

        for (i <- minRange to maxRange) {
          if (searchQtyrdd.contains(key1, key2, x.getString(salesidNo), key3, x.getString(itemNo), getWeek(currentYearweek, i))) {
            movingSum = movingSum + searchQtyrdd(key1, key2, x.getString(salesidNo), key3, x.getString(itemNo), getWeek(currentYearweek, i)).toInt
            movingSize = movingSize + 1
          }
        }

        if (movingSize != 0) {
          movingAvg = movingSum / movingSize
        }

        (x.getString(regionSeg1No),
          x.getString(regionSeg3No),
          x.getString(salesidNo),
          x.getString(productgroupNo),
          x.getString(itemNo),
          x.getString(yearweekNo),
          x.getString(yearNo),
          x.getString(weekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.getString(promotionYN),
          x.getString(moving_avgNo),
          x.getString(seasonalityNo),
          x.getString(fcstNo),
          x.getString(time_seriesNo)
        )
      })
      result
    })

    var movingAvgDF = mavg_data_rdd.toDF("regionseg", "regionseg3", "salesid", "productgroup", "item", "yearweek", "year", "week", "map_price", "ir", "pmap", "pmap10", "pro_percent", "qty", "promotionNY", "moving_avg", "seasonality", "fcst", "time_series")
    var movingAvg_data_rdd = movingAvgDF.rdd

    // 시즈널리티 계산
    var seasonality_data_rdd = movingAvg_data_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No), x.getString(productgroupNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var result = data.map(x => {
        var seasonality = 0.0d
        var currentQty = x.getString(qtyNo).toInt
        var movingAvg = x.getString(moving_avgNo).toDouble

        if (movingAvg != 0) {
          seasonality = currentQty / movingAvg
        }

        (x.getString(regionSeg1No),
          x.getString(regionSeg3No),
          x.getString(salesidNo),
          x.getString(productgroupNo),
          x.getString(itemNo),
          x.getString(yearweekNo),
          x.getString(yearNo),
          x.getString(weekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.getString(promotionYN),
          x.getString(moving_avgNo),
          x.getString(seasonalityNo),
          x.getString(fcstNo),
          x.getString(time_seriesNo)
        )
      })
      result
    })

    var refinedDF = seasonality_data_rdd.toDF("regionseg", "regionseg3", "salesid", "productgroup", "item", "yearweek", "year", "week", "map_price", "ir", "pmap", "pmap10", "pro_percent", "qty", "promotionNY", "moving_avg", "seasonality", "fcst", "time_series")
    var refinedDF_rdd = refinedDF.rdd

    // fcst, week_seasonality, time_series 산출
    var predict_rdd = refinedDF_rdd.groupBy(x => {
      (x.getString(regionSeg1No), x.getString(regionSeg3No), x.getString(salesidNo), x.getString(productgroupNo), x.getString(itemNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var key1 = key._1
      var key2 = key._2
      var key3 = key._3
      var key4 = key._4
      var key5 = key._5

      var yearweekArray = data.map(x => x.getString(yearweekNo)).toArray
      var groupMinYearweek = yearweekArray.min
      var groupMinYear = groupMinYearweek.substring(0, 4)
      var groupMaxYearweek = yearweekArray.max
      var groupMaxYear = groupMaxYearweek.substring(0, 4)

      var result = data.map(x => {
        var fcst = 0.0d
        var fcst_seasonality = 0.0d
        // 2. maxYearweek 부터 -4 까지의 평균 qty를 fcst에 넣어준다.
        var avg_4_sum = 0.0d
        var avg_4_seasonality = 0.0d
        var avg_4_size = 4
        var week_seasonality = x.getString(seasonalityNo).toDouble
        var week_seasonality_size = 0
        var time_series = 0.0d

        var groupCorrentYearweek = x.getString(yearweekNo)
        var groupCorrentWeek = groupCorrentYearweek.substring(4)

        if (x.getString(yearweekNo) > maxYearWeek) {
          // 1. fcst 컬럼 추가.
          for (i <- -(BASE_DATA) to 0) {
            if (searchQtyrdd.contains(key1, key2, key3, key4, key5, getWeek(maxYearWeek, i))) {
              avg_4_sum = avg_4_sum + searchQtyrdd(key1, key2, key3, key4, key5, getWeek(maxYearWeek, i)).toDouble
              avg_4_seasonality = avg_4_seasonality + searchSeasonalityrdd(key1, key2, key3, key4, key5, getWeek(maxYearWeek, i)).toDouble
            }
          }
          fcst = avg_4_sum / avg_4_size
          fcst_seasonality = avg_4_seasonality / avg_4_size
          // 3. 해당 주차의 평균 계절성지수를 구해주는 함수를 돌려 얻은 값을 seasonality에 넣어준다.
          for (i <- groupMinYear.toInt to (groupMaxYear.toInt - 1)) {
            var tempYearweek = i.toString + groupCorrentWeek

            if (searchSeasonalityrdd.contains(key1, key2, key3, key4, key5, tempYearweek)) {
              week_seasonality = week_seasonality + searchSeasonalityrdd(key1, key2, key3, key4, key5, tempYearweek).toDouble
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
          x.getString(yearweekNo),
          x.getString(yearNo),
          x.getString(weekNo),
          x.getString(map_priceNo),
          x.getString(irNo),
          x.getString(pmapNo),
          x.getString(pmap10No),
          x.getString(pro_percentNo),
          x.getString(qtyNo),
          x.getString(promotionYN),
          x.getString(moving_avgNo),
          week_seasonality.toString,
          fcst.toString,
          time_series.toString)
      })
      result
    })

    var final_result_rdd = predict_rdd.filter(x => {
      (x._6 >= getWeek(maxYearWeek, -(BASE_DATA)))
    })

    var final_result_DF = final_result_rdd.toDF("REGIONSEG", "REGIONSEG3", "SALESID", "PRODUCTGROUP", "ITEM", "YEARWEEK", "YEAR", "WEEK", "MAP_PRICE", "IR", "PMAP", "PMAP10", "PRO_PERCENT", "QTY", "PROMOTIONNY", "MOVING_AVG", "SEASONALITY", "FCST", "TIME_SERIES")
    // 데이터베이스 주소 및 접속정보 설정
    var outputUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
    var outputUser = "kopo"
    var outputPw = "kopo"

    // 데이터 저장
    var prop = new java.util.Properties
    prop.setProperty("driver", "oracle.jdbc.OracleDriver")
    prop.setProperty("user", outputUser)
    prop.setProperty("password", outputPw)
    var updeted_table = "common_group4_new"
    //append
    final_result_DF.write.mode("overwrite").jdbc(staticUrl, updeted_table, prop)

    print("Done.")
  }
}

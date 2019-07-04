package com.gang

object pj0703 {

  import java.text.SimpleDateFormat
  import java.util.Calendar
  import org.apache.spark.{SparkConf, SparkContext}
  import org.apache.spark.sql.{Row, SQLContext}

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataFrame").
      setMaster("local[4]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var salesFile = "pro_actual_sales.csv"
    var salesData =
      spark.read.format("csv").
        option("header", "true").
        option("encoding", "ms949").
        option("Delimiter", ",").
        load("c:/spark/bin/data/data/" + salesFile)

    var SalesRdd = salesData.rdd

    var salesDataColumns = salesData.columns
    var regionSeg1No = salesDataColumns.indexOf("regionSeg1")
    var productSeg2No = salesDataColumns.indexOf("productSeg2")
    var regionSeg2No = salesDataColumns.indexOf("regionSeg2")
    var regionSeg3No = salesDataColumns.indexOf("regionSeg3")
    var productSeg3No = salesDataColumns.indexOf("productSeg3")
    var yearweekNo = salesDataColumns.indexOf("yearweek")
    var yearNo = salesDataColumns.indexOf("year")
    var weekNo = salesDataColumns.indexOf("week")
    var qtyNo = salesDataColumns.indexOf("qty")

    var promotionFile = "pro_promotion.csv"
    var promotionData =
      spark.read.format("csv").
        option("header", "true").
        option("encoding", "ms949").
        option("Delimiter", ",").
        load("c:/spark/bin/data/data/" + promotionFile)

    var minYeaweek = promotionData.agg(min("planwee")).head.getString(0) // min(planwee) => 201601

    //DF데이터 필터(targetweek >= 201601) 후 rdd변환
    var filterPromotionData = promotionData.filter($"targetweek" >= minYeaweek)
    var PromotionRdd = filterPromotionData.rdd

    var promotionDataColumns = filterPromotionData.columns
    var regionsegNo = promotionDataColumns.indexOf("regionseg")
    var salesidNo = promotionDataColumns.indexOf("salesid")
    var productGroupNo = promotionDataColumns.indexOf("productgroup")
    var itemNo = promotionDataColumns.indexOf("item")
    var targetweekNo = promotionDataColumns.indexOf("targetweek")
    var planweeNo = promotionDataColumns.indexOf("planwee")
    var mapPriceNo = promotionDataColumns.indexOf("map_price")
    var irNo = promotionDataColumns.indexOf("ir")
    var pmapNo = promotionDataColumns.indexOf("pmap")
    var pmap10No = promotionDataColumns.indexOf("pmap10")
    var proPercentNo = promotionDataColumns.indexOf("pro_percent")

    //     mapPrice 값이 0인경우 mapPrice의 평균으로 대체하기 위해 productGroup, item 으로 그룹화
    //    var filterPromotionRdd = PromotionRdd.groupBy(x => {
    //      (x.getString(productGroupNo), x.getString(itemNo))
    //    }).flatMap(x => {
    //      var key = x._1
    //      var data = x._2
    //      var sumPrice = data.map(x => {
    //        x.getString(mapPriceNo).toInt
    //      }).sum
    //      var count_nz = data.filter(x => {
    //        var checkValid = false
    //        if (x.getString(mapPriceNo).toInt > 0) {
    //          checkValid = true
    //        }
    //        checkValid
    //      }).size
    //      //mapPrice 값이 모두 0인경우 count_nz 값(분모)이 0이므로 1로 수정
    //      if (count_nz == 0) {
    //        count_nz = 1
    //      }
    //      var avgPrice = (math.round(sumPrice / count_nz)).toInt
    //      var result = data.map(x => {
    //        var productGroup = x.getString(productGroupNo)
    //        var item = x.getString(itemNo)
    //        var targetweek = x.getString(targetweekNo)
    //        var mapPrice = x.getString(mapPriceNo).toInt
    //        var ir =  x.getString(irNo).toInt
    //        var pmap = x.getString(pmapNo).toInt
    //        var pmap10 = x.getString(pmap10No).toInt
    //        var pro_percent = x.getString(proPercentNo).toDouble
    // //mapPrice가 0이였던 데이터가 있는지 확인
    //        var checkZ = 0
    //        //mapPrice 값이 0인 경우 평균값으로 대체
    //        if (mapPrice <= 0) {
    //          mapPrice = avgPrice
    //          //평균값이 0인 경우 나머지 값도 0으로 대체
    //          if(mapPrice == 0){
    //            pmap = 0
    //            pmap10 = 0
    //            pro_percent = 0
    //          } else {
    //            pmap = mapPrice - ir
    //            pmap10 = math.round(pmap * 0.9).toInt
    //            pro_percent = 1 - (pmap10 / mapPrice.toDouble)
    //          }
    //          checkZ = 1
    //        }
    //        ((productGroup,item, targetweek), (mapPrice, ir, pmap, pmap10, pro_percent, checkZ))
    //      })
    //      result
    //    }).collectAsMap

    //     pro_actual_sales데이터와 조인 시 key로 잡을 productGroup, itemNo, targetweek로 그룹화하여
    //     mapPrice, ir, pmap, pmap10, pro_percent가 value로 나오도록 collectAsMap 수행
    var filterPromotionRdd = PromotionRdd.groupBy(x => {
      (x.getString(productGroupNo), x.getString(itemNo), x.getString(targetweekNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2
      var result = data.map(x => {
        (key, (x.getString(mapPriceNo).toInt, x.getString(irNo).toInt, x.getString(pmapNo),
          x.getString(pmap10No), x.getString(proPercentNo)))
      })
      result
    }).collectAsMap

    //Yearweek1, Yearweek2 사이의 전체 연주차를 배열로 return하는 함수
    def calWeek(Yearweek1: String, Yearweek2: String): Array[String] = {
      var yearweekArr: Array[String] = Array.empty[String]
      var maxYearweek, minYearweek = ""
      if (Yearweek1 > Yearweek2) {
        maxYearweek = Yearweek1
        minYearweek = Yearweek2
      } else {
        maxYearweek = Yearweek2
        minYearweek = Yearweek1
      }
      var maxYear = maxYearweek.substring(0, 4).toInt
      var minYear = minYearweek.substring(0, 4).toInt
      var maxWeek = maxYearweek.substring(4, 6).toInt
      var minWeek = minYearweek.substring(4, 6).toInt
      val calendar = Calendar.getInstance();
      calendar.setMinimalDaysInFirstWeek(4);
      calendar.setFirstDayOfWeek(Calendar.MONDAY);
      var dateFormat = new SimpleDateFormat("yyyyMMdd");
      while (minYear <= maxYear) {
        calendar.setTime(dateFormat.parse(minYear + "1231"));
        var currMaxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)
        if (maxYear == minYear) {
          currMaxWeek = maxWeek
        }
        while (minWeek <= currMaxWeek) {
          var calWeek = minWeek.toString
          if (minWeek < 10) {
            calWeek = 0 + calWeek
          }
          yearweekArr ++= Array(minYear.toString + calWeek)
          minWeek = minWeek + 1
        }
        minYear = minYear + 1
        minWeek = 1
      }
      return yearweekArr
    }

    //빈 주차만 포함한 rdd를 구함
    var salesEmptyRdd = SalesRdd.groupBy(x => {
      (x.getString(productSeg2No), x.getString(regionSeg3No), x.getString(productSeg3No))
    }).
      flatMap(x => {
        var key = x._1
        var data = x._2
        var maxYearweek = data.map(x => {
          x.getString(6)
        }).max
        var minYearweek = data.map(x => {
          x.getString(6)
        }).min
        var existWeekArr = data.map(x => {
          x.getString(6)
        }).toArray
        var wholeWeekArr = calWeek(maxYearweek, minYearweek)
        var emptyWeekArr = wholeWeekArr.diff(existWeekArr)
        var regionSeg1 = data.map(x => {
          x.getString(0)
        }).head
        var regionSeg2 = data.map(x => {
          x.getString(3)
        }).head
        var productSeg1 = data.map(x => {
          x.getString(1)
        }).head
        var dataSize = emptyWeekArr.size
        var resultArr = Array.fill(dataSize)(Row("REGIONSEG1", productSeg1, "PRODUCTSEG2", "REGIONSEG2", "REGIONSEG3", "PRODUCTSEG3", "YEARWEEK", "YEAR", "WEEK", "0"))
        for (i <- 0 until dataSize) {
          var emptyYearweek = emptyWeekArr(i)
          var emptyYear = emptyYearweek.substring(0, 4)
          var emptyWeek = emptyYearweek.substring(4, 6)
          resultArr(i) = Row(regionSeg1, productSeg1, key._1, regionSeg2, key._2, key._3, emptyYearweek, emptyYear, emptyWeek, "0")
        }
        resultArr
      })

    //기존 salesRdd와 빈주차만 포함된 salesEmptyRdd를 더함
    var finalSalesRdd = salesEmptyRdd ++ SalesRdd


    //pro_actual_sales데이터와 filterPromotionRdd의 value 데이터를 합침
    var finalResultRdd = finalSalesRdd.map(x => {
      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)
      var year = x.getString(yearNo)
      var week = x.getString(weekNo)
      var qty = x.getString(qtyNo)
      var mapPrice = null.asInstanceOf[Int] //null 값의 type 지정가능
      var ir = null.asInstanceOf[Int] // 0(int), 0.0(double)으로 출력
      var pmap = null.asInstanceOf[Int]
      var pmap10 = null.asInstanceOf[Int]
      var pro_percent = null.asInstanceOf[Double]
      var promotion_yn = 0
      //mapPrice가 0이였던 데이터가 있는지 확인 있을경우 1
      //      var checkZ = 0

      //null 값을 초기 값으로 지정하고 promotionGroupRdd의 key 값이 productSeg2, productSeg3, yearweek와 같은 경우
      //promotionGroupRdd의 value 값을 각 변수에 넣음
      if (filterPromotionRdd.contains(productSeg2, productSeg3, yearweek)) {
        mapPrice = filterPromotionRdd(productSeg2, productSeg3, yearweek)._1.toInt
        ir = filterPromotionRdd(productSeg2, productSeg3, yearweek)._2.toInt
        pmap = filterPromotionRdd(productSeg2, productSeg3, yearweek)._3.toInt
        pmap10 = filterPromotionRdd(productSeg2, productSeg3, yearweek)._4.toInt
        pro_percent = filterPromotionRdd(productSeg2, productSeg3, yearweek)._5.toDouble
        promotion_yn = 1
        //        checkZ = filterPromotionRdd(productSeg2, productSeg3, yearweek)._6
      }
      (regionSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, yearweek, year, week, qty,
        mapPrice, ir, pmap, pmap10, pro_percent, promotion_yn)
    })

    //productSeg2, regionSeg3, productSeg3, yearweek를 key로 정렬
    // - > 정렬을 해야 이동평균, 계절성 지수를 제대로 구함
    var sortedFinalRdd = finalResultRdd.sortBy(x => (x._2, x._4, x._5, x._6))

    //productSeg2, regionSeg3, productSeg3로 그룹화하여 이동평균, 계절성 지수를 구함
    var mvAvgFinalRdd = sortedFinalRdd.groupBy(x => (x._2, x._4, x._5)).flatMap(x => {
      var key = x._1
      var data = x._2
      var regionSeg1 = data.map(x => {x._1}).head
      var productSeg2 = data.map(x => {x._2}).head
      var regionSeg2 = data.map(x => {x._3}).head
      var regionSeg3 = data.map(x => {x._4}).head
      var productSeg3 = data.map(x => {x._5}).head
      var yearweek = data.map(x => {x._6}).toArray
      var year = data.map(x => {x._7}).toArray
      var week = data.map(x => {x._8}).toArray
      var qtyArr = data.map(x => {x._9.toInt}).toArray
      var mapPrice = data.map(x => {x._10}).toArray
      var ir = data.map(x => {x._11}).toArray
      var pmap = data.map(x => {x._12}).toArray
      var pmap10 = data.map(x => {x._13}).toArray
      var pro_percent = data.map(x => {x._14}).toArray
      var promotion_yn = data.map(x => {x._15}).toArray

      var dataSize = qtyArr.size
      var resultArr = Array.fill(dataSize)("regionSeg1", "productSeg2", "regionSeg2", "regionSeg3", "productSeg3", "yearweek", "year", "week", "qty", 0,0,0,0,0.0d,0,0.0d,0.0d)

      var rollingSize = 5
      var calRange = (rollingSize - 1) / 2

      for (i <- 0 until dataSize) {
        var qtySum = 0
        var count = 0
        var j = i - calRange
        if (j < 0) {
          j = 0
        }
        while (j <= i + calRange) {
          qtySum = qtySum + qtyArr(j)
          count = count + 1
          j = j + 1
          //인덱스(j)가 dataSize와 같아지는 경우 인덱스에 calRange를 더해 while 종료 => break
          if (j >= dataSize) {
            j = j + calRange
          }
        }
        var mvAvg = (qtySum.toDouble / count)
        var ratio = 0.0d
        if(mvAvg != 0){
          ratio = qtyArr(i) / mvAvg
        }
        resultArr(i) = (regionSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, yearweek(i), year(i), week(i), qtyArr(i).toString, mapPrice(i), ir(i), pmap(i), pmap10(i), pro_percent(i),promotion_yn(i), mvAvg,  ratio)
      }
      resultArr
    })

    var FinalRdd = mvAvgFinalRdd.sortBy(x => (x._2, x._4, x._5, x._6))

    var finalResultDF = FinalRdd.toDF("regionSeg1", "productSeg2", "regionSeg2", "regionSeg3", "productSeg3",
      "yearweek", "year", "week", "qty", "mapPrice", "ir", "pmap", "pmap10", "pro_percent", "promotion_yn", "mvAvg", "ratio")

    finalResultDF.
      coalesce(1). // 파일개수
      write.format("csv"). // 저장포맷
      mode("overwrite"). // 저장모드 append/overwrite
      option("header", "true"). // 헤더 유/무
      save("c:/spark/bin/data/refine_pro_promotion.csv") // 저장파일명
  }

}

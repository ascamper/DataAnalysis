package com.pjw

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.parquet.format.IntType
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object dataAnalysis_190702_PJW {
  def main(args: Array[String]): Unit = {
    var rawFile = "pro_promotion.csv"

    var pro_promotion =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + rawFile)

    var pro_promotion_rdd = pro_promotion.rdd

    var pp_columns = pro_promotion.columns
    var pp_regionsegNo = pp_columns.indexOf("regionseg")
    var pp_salesidNo = pp_columns.indexOf("salesid")
    var pp_productgroupNo = pp_columns.indexOf("productgroup")
    var pp_itemNo = pp_columns.indexOf("item")
    var pp_targetweekNo = pp_columns.indexOf("targetweek")
    var pp_planweekNo = pp_columns.indexOf("planweek")
    var pp_map_priceNo = pp_columns.indexOf("map_price")
    var pp_irNo = pp_columns.indexOf("ir")
    var pp_pmapNo = pp_columns.indexOf("pmap")
    var pp_pmap10No = pp_columns.indexOf("pmap10")
    var pp_pro_percentNo = pp_columns.indexOf("pro_percent")

    var filtered_pro_promotion_rdd = pro_promotion_rdd.filter(x => {
      ((x.getString(pp_targetweekNo) >= x.getString(pp_planweekNo)))
    })

    var group_pro_promotion_rdd = filtered_pro_promotion_rdd.groupBy(x => {
      (x.getString(pp_regionsegNo), x.getString(pp_salesidNo), x.getString(pp_productgroupNo), x.getString(pp_itemNo))
    }).flatMap(x => {
      var key = x._1
      var data = x._2

      var sum = data.map(x => {
        x.getString(pp_map_priceNo).toInt
      }).sum

      var size = data.map(x => {
        (x.getString(pp_map_priceNo))
      }).size

      var zero_size = data.filter(x => {
        (x.getString(pp_map_priceNo).toInt == 0)
      }).size

      var result = data.map(x => {
        var regionseg = x.getString(pp_regionsegNo)
        var salesid = x.getString(pp_salesidNo)
        var productgroup = x.getString(pp_productgroupNo)
        var item = x.getString(pp_itemNo)
        var targetweek = x.getString(pp_targetweekNo)
        var planweek = x.getString(pp_planweekNo)
        var map_price = x.getString(pp_map_priceNo).toInt
        var ir = x.getString(pp_irNo).toInt
        var pmap = x.getString(pp_pmapNo).toDouble
        var pmap10 = x.getString(pp_pmap10No).toDouble
        var pro_percent = x.getString(pp_pro_percentNo).toDouble

        var avg_price = 0
        var totalSize = size - zero_size

        if (totalSize != 0 && sum != 0) {
          avg_price = sum / totalSize
        }

        map_price = avg_price

        if (map_price != 0) {
          pmap = map_price - ir
          pmap10 =  Math.round(pmap * 0.9)
          pro_percent = (1 - pmap10 / map_price)
        }
        else{
          pmap = 0
          pmap10 = 0
          ir = 0
          pro_percent = 0
        }

        (key._1, key._2, key._3, key._4, targetweek, planweek, map_price, ir, pmap, pmap10, pro_percent)
      })
      result
    })

    group_pro_promotion_rdd.first

    var refined_pro_promotion = group_pro_promotion_rdd.toDF("regionSeg", "salesid", "productgroup", "item", "targetweek", "planweek", "map_price",
                                                "ir", "pmap", "pmap10", "pro_percent")

    // 제대로 뽑아졌는지 확인 하기 위한 csv 추출
//    refined_pro_promotion.
//      coalesce(1).
//      write.format("csv").
//      mode("overwrite").
//      option("header", "true").
//      save("c:/resultDF.csv")

    // 3. pro_actual_sales rdd 생성
    var rawFile1 = "pro_actual_sales.csv"

    var pro_actual_sales =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + rawFile1)

    var pro_actual_sales_rdd = pro_actual_sales.rdd

    //  데이터 불러온 후 컬럼 인덱싱 처리 한 번 해줘
    var pas_columns = pro_actual_sales.columns
    var pas_regionSeg1No = pas_columns.indexOf("regionSeg1")
    var pas_productSeg1No = pas_columns.indexOf("productSeg1")
    var pas_productSeg2No = pas_columns.indexOf("productSeg2")
    var pas_regionSeg2No = pas_columns.indexOf("regionSeg2")
    var pas_regionSeg3No = pas_columns.indexOf("regionSeg3")
    var pas_productSeg3No = pas_columns.indexOf("productSeg3")
    var pas_yearweekNo = pas_columns.indexOf("yearweek")
    var pas_yearNo = pas_columns.indexOf("year")
    var pas_weekNo = pas_columns.indexOf("week")
    var pas_qtyNo = pas_columns.indexOf("qty")

    // 4. rdd -> df

    // 5. join with sql
    refined_pro_promotion.createOrReplaceTempView("refined_pro_promotion")
    pro_actual_sales.createOrReplaceTempView("pro_actual_sales")

    var joinResultDf = spark.sql(
      """select
        |    a.regionseg1 as regionseg,
        |    a.productseg1,
        |    a.productseg2 as productgroup,
        |    a.regionseg2 as salesid,
        |    a.regionseg3,
        |    a.productseg3 as item,
        |    a.yearweek,
        |    nvl(b.map_price,0) as map_price,
        |    nvl(b.ir,0) as ir,
        |    nvl(b.pmap,0) as pmap,
        |    nvl(b.pmap10,0) as pmap10,
        |    nvl(b.pro_percent,0) as pro_percent,
        |    a.qty
        |from  pro_actual_sales a
        |left join refined_pro_promotion b
        |on a.regionseg1 = b.regionseg
        |and a.regionseg2 = b.salesid
        |and a.productseg2 = b.productgroup
        |and a.productseg3 = b.item
        |and a.yearweek = b.targetweek
      """)
    // 확인을 위한 csv 추출
    joinResultDf.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/joinedDF.csv")

    // 그룹별 최소주차부터 최대주차까지 qty = 0 채우기

    // week계산 함수 가져오기
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

    var joinResultDf_rdd = joinResultDf.rdd

    var join_columns = joinResultDf.columns
    var join_regionSeg1No = join_columns.indexOf("regionseg")
    var join_productSeg1No = join_columns.indexOf("productseg1")
    var join_productgroupNo = join_columns.indexOf("productgroup")
    var join_salesidNo = join_columns.indexOf("salesid")
    var join_regionSeg3No = join_columns.indexOf("regionseg3")
    var join_itemNo = join_columns.indexOf("item")
    var join_yearweekNo = join_columns.indexOf("yearweek")
    var join_map_priceNo = join_columns.indexOf("map_price")
    var join_irNo = join_columns.indexOf("ir")
    var join_pmapNo = join_columns.indexOf("pmap")
    var join_pmap10No = join_columns.indexOf("pmap10")
    var join_pro_percentNo = join_columns.indexOf("pro_percent")
    var join_qtyNo = join_columns.indexOf("qty")

    var filled_joinResultDf_rdd = joinResultDf_rdd.groupBy( x => {(x.getString(join_regionSeg1No), x.getString(join_productSeg1No)
                                                            , x.getString(join_productgroupNo), x.getString(join_salesidNo)
                                                            , x.getString(join_regionSeg3No), x.getString(join_itemNo))
    }).flatMap( x => {
      var key = x._1
      var data = x._2

      var weekArray = data.map( x => {x.getString(join_yearweekNo)}).toArray
      var minWeek = weekArray.min
      var maxWeek = weekArray.max

      var fullWeekArray = Array(minWeek)
      var currentWeek = minWeek

      while(maxWeek != currentWeek ){
        currentWeek = getWeek(currentWeek, 1)
        fullWeekArray ++= Array(currentWeek)
      }

      var refinedWeekArray = fullWeekArray.diff(weekArray)

      var newWeekMap = refinedWeekArray.map( x => {
        (key._1, key._2, key._3, key._4, key._5, key._6, x, "0",0,0,0,0,"0")
      })

      // null 처리. 데이터 타입 다 맞출것 제발 제발
      var origin = data.map( x => {
        (key._1,
        key._2,
        key._3,
        key._4,
        key._5,
        key._6,
       x.getString(join_yearweekNo),
       x.getInt(join_map_priceNo).toString,
       x.getInt(join_irNo).toString,
       x.getDouble(join_pmapNo),
       x.getDouble(join_pmap10No),
       x.getDouble(join_pro_percentNo),
       x.getString(join_qtyNo)
        )
      })

      var resultMap = newWeekMap ++ origin

      resultMap
    }).sortBy(x => (x._1,x._3,x._4,x._5,x._6))

    filled_joinResultDf_rdd.collect.foreach(println)
    // 총 카운트 : 189717

    // 확인을 위해 csv 추출
    var filled_joinResultDf = filled_joinResultDf_rdd.toDF("regionseg", "productseg1","productgroup","salesid","regionseg3","item"
                                                      ,"yearweek","map_price","ir","pmap","pmap10","pro_percent","qty")
    filled_joinResultDf.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/joinedDF.csv")

    // promotionYN 추가
    var refinedDF_rdd = filled_joinResultDf_rdd.map( x => {

      var promotionNY = "Y"

      if(x._12 == 0) {
        promotionNY = "N"
      }
      (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9.toString, x._10.toString, x._11.toString, x._12.toString, x._13, promotionNY)
    })

    //테스트를 위해 추출
    var refinedDF = refinedDF_rdd.toDF("regionseg", "productseg1","productgroup","salesid","regionseg3","item"
      ,"yearweek","map_price","ir","pmap","pmap10","pro_percent","qty","promotionNY")

    refinedDF.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/refinedDF.csv")

  }
}
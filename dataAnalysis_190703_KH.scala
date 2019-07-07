package com.hyun

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import scala.collection.mutable.ArrayBuffer

object Hyun {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setAppName("DataLoading").
      setMaster("local[*]")
    var sc = new SparkContext(conf)
    val spark = new SQLContext(sc)
    import spark.implicits._

    var actualSales =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/pro_actual_sales.csv")
    var promotion =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/pro_promotion.csv")

    var actualSalesColumns = actualSales.columns
    var promotionColumns = promotion.columns

    var regionSeg1No = actualSalesColumns.indexOf("regionSeg1")
    var productSeg1No = actualSalesColumns.indexOf("productSeg1")
    var productSeg2No = actualSalesColumns.indexOf("productSeg2")
    var regionSeg2No = actualSalesColumns.indexOf("regionSeg2")
    var regionSeg3No = actualSalesColumns.indexOf("regionSeg3")
    var productSeg3No = actualSalesColumns.indexOf("productSeg3")
    var yearweekNo = actualSalesColumns.indexOf("yearweek")
    var yearNo = actualSalesColumns.indexOf("year")
    var weekNo = actualSalesColumns.indexOf("week")
    var qtyNo = actualSalesColumns.indexOf("qty")

    var regionsegNo = promotionColumns.indexOf("regionseg")
    var salesidNo = promotionColumns.indexOf("salesid")
    var productgroupNo = promotionColumns.indexOf("productgroup")
    var itemNo = promotionColumns.indexOf("item")
    var targetweekNo = promotionColumns.indexOf("targetweek")
    var planweeNo = promotionColumns.indexOf("planwee")
    var map_priceNo = promotionColumns.indexOf("map_price")
    var irNo = promotionColumns.indexOf("ir")
    var pmapNo = promotionColumns.indexOf("pmap")
    var pmap10No = promotionColumns.indexOf("pmap10")
    var pro_percentNo = promotionColumns.indexOf("pro_percent")

    var actualSalesRdd = actualSales.rdd
    var promotionRdd = promotion.rdd

    var filterPromotionRdd = promotionRdd.filter(x => {

      var targetweek = x.getString(targetweekNo)
      var planwee = x.getString(planweeNo)

      targetweek >= planwee

    })

    var fillPromotionRdd = filterPromotionRdd.groupBy(x => {

      var regionseg = x.getString(regionsegNo)
      var salesid = x.getString(salesidNo)
      var productgroup = x.getString(productgroupNo)
      var item = x.getString(itemNo)

      (regionseg,
        salesid,
        productgroup,
        item)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var filterData = data.filter(x => {

        var map_price = x.getString(map_priceNo).toDouble

        map_price > 0

      })

      var size = filterData.size.toDouble

      var sum = filterData.map(x => {

        var map_price = x.getString(map_priceNo).toDouble

        map_price

      }).sum

      var avg = if (size > 0) {

        sum / size

      } else {

        0d

      }

      var result = data.map(x => {

        var regionseg = x.getString(regionsegNo)
        var salesid = x.getString(salesidNo)
        var productgroup = x.getString(productgroupNo)
        var item = x.getString(itemNo)
        var targetweek = x.getString(targetweekNo)
        var planwee = x.getString(planweeNo)
        var map_price = x.getString(map_priceNo).toDouble
        var ir = x.getString(irNo).toDouble
        var pmap = x.getString(pmapNo).toDouble
        var pmap10 = x.getString(pmap10No).toDouble
        var pro_percent = x.getString(pro_percentNo).toDouble

        if (map_price == 0) {

          map_price = avg

          if (map_price == 0) {

            ir = 0d
            pmap = 0d
            pmap10 = 0d
            pro_percent = 0d

          } else {

            pmap = map_price - ir
            pmap10 = pmap * 0.9
            pro_percent = 1 - (pmap10 / map_price)

          }

        }

        Row(regionseg,
          salesid,
          productgroup,
          item,
          targetweek,
          planwee,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent)

      })

      result

    })

    var collectPromotionRdd = fillPromotionRdd.groupBy(x => {

      var regionseg = x.getString(regionsegNo)
      var salesid = x.getString(salesidNo)
      var productgroup = x.getString(productgroupNo)
      var item = x.getString(itemNo)
      var targetweek = x.getString(targetweekNo)

      (regionseg,
        salesid,
        productgroup,
        item,
        targetweek)

    }).map(x => {

      var key = x._1
      var data = x._2

      var result = data.map(x => {

        var map_price = x.getDouble(map_priceNo)
        var ir = x.getDouble(irNo)
        var pmap = x.getDouble(pmapNo)
        var pmap10 = x.getDouble(pmap10No)
        var pro_percent = x.getDouble(pro_percentNo)

        (map_price,
          ir,
          pmap,
          pmap10,
          pro_percent)

      })

      (key, result.head)

    }).collectAsMap()

    var leftjoinActualSales = actualSalesRdd.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var regionSeg2 = x.getString(regionSeg2No)
      var productSeg2 = x.getString(productSeg2No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        regionSeg2,
        productSeg2,
        productSeg3,
        yearweek)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var map_price = 0d
      var ir = 0d
      var pmap = 0d
      var pmap10 = 0d
      var pro_percent = 0d
      var promotionYN = "N"

      if (collectPromotionRdd.contains(key)) {

        map_price = collectPromotionRdd(key)._1
        ir = collectPromotionRdd(key)._2
        pmap = collectPromotionRdd(key)._3
        pmap10 = collectPromotionRdd(key)._4
        pro_percent = collectPromotionRdd(key)._5

        if (map_price != 0) {

          promotionYN = "Y"

        }

      }

      var result = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)
        var productSeg1 = x.getString(productSeg1No)
        var productSeg2 = x.getString(productSeg2No)
        var regionSeg2 = x.getString(regionSeg2No)
        var regionSeg3 = x.getString(regionSeg3No)
        var productSeg3 = x.getString(productSeg3No)
        var yearweek = x.getString(yearweekNo)
        var year = x.getString(yearNo)
        var week = x.getString(weekNo)
        var qty = x.getString(qtyNo)

        Row(regionSeg1,
          productSeg1,
          productSeg2,
          regionSeg2,
          regionSeg3,
          productSeg3,
          yearweek,
          year,
          week,
          qty,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent,
          promotionYN)

      })

      result

    })

    map_priceNo += 4
    irNo += 4
    pmapNo += 4
    pmap10No += 4
    pro_percentNo += 4
    var promotionYNNo = pro_percentNo + 1

    def preWeek(inputYearWeek: String, gapWeek: Int): String = {

      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(currYear + "1231"))

      if (currWeek <= gapWeek) {

        var iterGap = gapWeek - currWeek
        var iterYear = currYear - 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {

          if (iterWeek <= iterGap) {

            iterGap = iterGap - iterWeek
            iterYear = iterYear - 1

            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

          } else {

            iterWeek = iterWeek - iterGap
            iterGap = 0

          }

        }

        return iterYear.toString + "%02d".format(iterWeek)

      } else {

        var resultYear = currYear
        var resultWeek = currWeek - gapWeek

        return resultYear.toString + "%02d".format(resultWeek)

      }

    }

    def postWeek(inputYearWeek: String, gapWeek: Int): String = {

      val currYear = inputYearWeek.substring(0, 4).toInt
      val currWeek = inputYearWeek.substring(4, 6).toInt

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")
      calendar.setTime(dateFormat.parse(currYear + "1231"))

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek + gapWeek) {

        var iterGap = gapWeek + currWeek - maxWeek
        var iterYear = currYear + 1

        calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

        var iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

        while (iterGap > 0) {

          if (iterWeek < iterGap) {

            iterGap = iterGap - iterWeek
            iterYear = iterYear + 1

            calendar.setTime(dateFormat.parse(iterYear.toString() + "1231"))

            iterWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

          } else {

            iterWeek = iterGap
            iterGap = 0

          }

        }

        return iterYear.toString() + "%02d".format(iterWeek)

      } else {

        return currYear.toString() + "%02d".format((currWeek + gapWeek))

      }

    }

    def lagWeek(week1: String, week2: String): Int = {

      var fromWeek = ""
      var toWeek = ""

      if (week1.toInt < week2.toInt) {

        fromWeek = week1
        toWeek = week2

      } else {

        fromWeek = week2
        toWeek = week1

      }

      val calendar = Calendar.getInstance()
      calendar.setMinimalDaysInFirstWeek(4)
      calendar.setFirstDayOfWeek(Calendar.MONDAY)
      var dateFormat = new SimpleDateFormat("yyyyMMdd")

      var currYear = fromWeek.substring(0, 4).toInt
      var currWeek = fromWeek.substring(4, 6).toInt

      calendar.setTime(dateFormat.parse(currYear + "1231"))

      var maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek) {

        return 0

      }

      currYear = toWeek.substring(0, 4).toInt
      currWeek = toWeek.substring(4, 6).toInt

      calendar.setTime(dateFormat.parse(currYear + "1231"))

      maxWeek = calendar.getActualMaximum(Calendar.WEEK_OF_YEAR)

      if (maxWeek < currWeek) {

        return 0

      }

      var calcGapNum = 0
      var calcWeek = toWeek

      while (fromWeek.toInt != calcWeek.toInt) {

        calcGapNum = calcGapNum + 1
        calcWeek = preWeek(toWeek, calcGapNum)

      }

      calcGapNum

    }

    var fillActualSales = leftjoinActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var start = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).min.toString

      var end = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).max.toString

      var gap = lagWeek(start, end)
      var allYearweekArray = Array.empty[String]
      var i = 0

      while (i <= gap) {

        allYearweekArray ++= Array(postWeek(start, i))
        i += 1

      }

      var yearweekArray = data.map(x => {

        var yearweek = x.getString(yearweekNo)

        yearweek

      }).toArray

      var diffYearweekArray = allYearweekArray.diff(yearweekArray)

      var regionSeg1 = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)

        regionSeg1

      }).head

      var productSeg1 = data.map(x => {

        var productSeg1 = x.getString(productSeg1No)

        productSeg1

      }).head

      var productSeg2 = data.map(x => {

        var productSeg2 = x.getString(productSeg2No)

        productSeg2

      }).head

      var regionSeg2 = data.map(x => {

        var regionSeg2 = x.getString(regionSeg2No)

        regionSeg2

      }).head

      var regionSeg3 = data.map(x => {

        var regionSeg3 = x.getString(regionSeg3No)

        regionSeg3

      }).head

      var productSeg3 = data.map(x => {

        var productSeg3 = x.getString(productSeg3No)

        productSeg3

      }).head

      var insertData = new ArrayBuffer[org.apache.spark.sql.Row]

      i = 0

      while (i < diffYearweekArray.size) {

        var yearweek = diffYearweekArray(i)
        var year = yearweek.substring(0, 4)
        var week = yearweek.substring(4, 6)

        insertData.append(Row(regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, yearweek, year, week, "0", 0d, 0d, 0d, 0d, 0d, "N"))

        i += 1

      }

      data ++ insertData.toIterable

    })

    var sortActualSales = fillActualSales.sortBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3,
        yearweek)

    })

    var collectActualSales = sortActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3,
        yearweek)

    }).map(x => {

      var key = x._1
      var data = x._2

      var qty = data.map(x => {

        var qty = x.getString(qtyNo)

        qty.toDouble

      }).head

      (key, qty)

    }).collectAsMap()

    var seasonalityActualSales = sortActualSales.groupBy(x => {

      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg1 = x.getString(productSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)

      (regionSeg1,
        productSeg1,
        productSeg2,
        regionSeg2,
        regionSeg3,
        productSeg3)

    }).flatMap(x => {

      var key = x._1
      var data = x._2

      var seasonalityArray = data.map(x => {

        var regionSeg1 = x.getString(regionSeg1No)
        var productSeg1 = x.getString(productSeg1No)
        var productSeg2 = x.getString(productSeg2No)
        var regionSeg2 = x.getString(regionSeg2No)
        var regionSeg3 = x.getString(regionSeg3No)
        var productSeg3 = x.getString(productSeg3No)
        var yearweek = x.getString(yearweekNo)
        var qty = x.getString(qtyNo).toDouble

        var yearweekArray = Array(yearweek)

        for (i <- (1 to 2)) {

          yearweekArray ++= Array(preWeek(yearweek, i))
          yearweekArray ++= Array(postWeek(yearweek, i))

        }

        var qtyArray = Array.empty[Double]

        for (i <- yearweekArray) {

          var key = (regionSeg1, productSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, i)

          if (collectActualSales.contains(key)) {

            qtyArray ++= Array(collectActualSales(key))

          }

        }

        var size = qtyArray.size
        var sum = qtyArray.sum
        var avg = sum / size
        var seasonality = if (avg != 0) {

          qty / avg

        } else {

          0d

        }

        seasonality

      }).toArray

      var zipData = data.zipWithIndex

      var result = zipData.map(x => {

        var data = x._1
        var index = x._2

        var regionSeg1 = data.getString(regionSeg1No)
        var productSeg1 = data.getString(productSeg1No)
        var productSeg2 = data.getString(productSeg2No)
        var regionSeg2 = data.getString(regionSeg2No)
        var regionSeg3 = data.getString(regionSeg3No)
        var productSeg3 = data.getString(productSeg3No)
        var yearweek = data.getString(yearweekNo)
        var year = data.getString(yearNo)
        var week = data.getString(weekNo)
        var qty = data.getString(qtyNo)
        var map_price = data.getDouble(map_priceNo)
        var ir = data.getDouble(irNo)
        var pmap = data.getDouble(pmapNo)
        var pmap10 = data.getDouble(pmap10No)
        var pro_percent = data.getDouble(pro_percentNo)
        var promotionYN = data.getString(promotionYNNo)

        var seasonality = seasonalityArray(index)

        Row(regionSeg1,
          productSeg1,
          productSeg2,
          regionSeg2,
          regionSeg3,
          productSeg3,
          yearweek,
          year,
          week,
          qty,
          map_price,
          ir,
          pmap,
          pmap10,
          pro_percent,
          promotionYN,
          seasonality)

      })

      result

    })

    seasonalityActualSales.foreach(println)

//    var forecastActualSales = seasonalityActualSales.groupBy(x => {
//
//      var regionSeg1 = x.getString(regionSeg1No)
//      var productSeg1 = x.getString(productSeg1No)
//      var productSeg2 = x.getString(productSeg2No)
//      var regionSeg2 = x.getString(regionSeg2No)
//      var regionSeg3 = x.getString(regionSeg3No)
//      var productSeg3 = x.getString(productSeg3No)
//
//      (regionSeg1,
//        productSeg1,
//        productSeg2,
//        regionSeg2,
//        regionSeg3,
//        productSeg3)
//
//    })

  }

}
package com.gang

import java.text.SimpleDateFormat
import java.util.Calendar

import oracle.net.aso.i
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object ProjectWork {
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

    var maxYear = salesData.agg(max("year")).head.getString(0) // max(year) => 2016

    //DF데이터 필터(year == 2016) 후 rdd변환
    var filterSalesData = salesData.filter($"year" === maxYear)
    var filterSalesRdd = filterSalesData.rdd

    var salesDataColumns = filterSalesData.columns
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
    var minPrice = promotionData.agg(min("map_price")).head.getString(0) // min(mapPrice) => 0

    //DF데이터 필터(targetweek >= 201601, map_price > 0) 후 rdd변환
    var filterPromotionData = promotionData.filter(($"targetweek" >= minYeaweek) && ($"map_price" > minPrice))
    var filterPromotionRdd = filterPromotionData.rdd

    var promotionDataColumns = filterPromotionData.columns
    var productGroupNo = promotionDataColumns.indexOf("productgroup")
    var itemNo = promotionDataColumns.indexOf("item")
    var targetweekNo = promotionDataColumns.indexOf("targetweek")
    var mapPriceNo = promotionDataColumns.indexOf("map_price")
    var irNo = promotionDataColumns.indexOf("ir")
    var pmapNo = promotionDataColumns.indexOf("pmap")
    var pmap10No = promotionDataColumns.indexOf("pmap10")
    var proPercentNo = promotionDataColumns.indexOf("pro_percent")

    // pro_actual_sales데이터와 조인 시 key로 잡을 productGroup, itemNo, targetweek로 그룹화하여
    // mapPrice, ir, pmap, pmap10, pro_percent가 value로 나오도록 collectAsMap 수행
    var promotionGroupRdd = filterPromotionRdd.groupBy(x => {
      (x.getString(productGroupNo), x.getString(itemNo), x.getString(targetweekNo))
    }).
      flatMap(x => {
        var key = x._1
        var data = x._2
        var result = data.map(x => {
          (key, (x.getString(mapPriceNo).toInt, x.getString(irNo).toInt, x.getString(pmapNo),
            x.getString(pmap10No), x.getString(proPercentNo)))
        })
        result
      }).collectAsMap

    //pro_actual_sales데이터와 promotionGroupRdd의 value 데이터를 합침
    var finalResultRdd = filterSalesRdd.map(x => {
      var regionSeg1 = x.getString(regionSeg1No)
      var productSeg2 = x.getString(productSeg2No)
      var regionSeg2 = x.getString(regionSeg2No)
      var regionSeg3 = x.getString(regionSeg3No)
      var productSeg3 = x.getString(productSeg3No)
      var yearweek = x.getString(yearweekNo)
      var year = x.getString(yearNo)
      var week = x.getString(weekNo)
      var qty = x.getString(qtyNo).toInt
      var mapPrice = null.asInstanceOf[Int] //null 값의 type 지정가능
      var ir = null.asInstanceOf[Int] // 0(int), 0.0(double)으로 출력
      var pmap = null.asInstanceOf[Int]
      var pmap10 = null.asInstanceOf[Int]
      var pro_percent = null.asInstanceOf[Double]

      //null 값을 초기 값으로 지정하고 promotionGroupRdd의 key 값이 productSeg2, productSeg3, yearweek와 같은 경우
      //promotionGroupRdd의 value 값을 각 변수에 넣음
      if (promotionGroupRdd.contains(productSeg2, productSeg3, yearweek)) {
        mapPrice = promotionGroupRdd(productSeg2, productSeg3, yearweek)._1.toInt
        ir = promotionGroupRdd(productSeg2, productSeg3, yearweek)._2.toInt
        pmap = promotionGroupRdd(productSeg2, productSeg3, yearweek)._3.toInt
        pmap10 = promotionGroupRdd(productSeg2, productSeg3, yearweek)._4.toInt
        pro_percent = promotionGroupRdd(productSeg2, productSeg3, yearweek)._5.toDouble
      }
      (regionSeg1, productSeg2, regionSeg2, regionSeg3, productSeg3, yearweek, year, week, qty,
        mapPrice, ir, pmap, pmap10, pro_percent)
    })


    var finalResultDF = finalResultRdd.toDF("regionSeg1", "productSeg2", "regionSeg2", "regionSeg3", "productSeg3",
      "yearweek", "year", "week", "qty", "mapPrice", "ir", "pmap", "pmap10", "pro_percent")


  }
}

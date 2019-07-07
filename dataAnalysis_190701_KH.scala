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
      var qty = x.getString(qtyNo).toDouble

      (targetweek >= planwee
        && qty > 0)

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

      if (collectPromotionRdd.contains(key)) {

        map_price = collectPromotionRdd(key)._1
        ir = collectPromotionRdd(key)._2
        pmap = collectPromotionRdd(key)._3
        pmap10 = collectPromotionRdd(key)._4
        pro_percent = collectPromotionRdd(key)._5

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
          pro_percent)

      })

      result

    })

  }

}
package com.pjw

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import spark.implicits._

object dataAnalysis_190705_PJW {
  def main(args: Array[String]): Unit = {
    var csvFile = "final_data_new_2.csv"

    var final_data =
      spark.read.format("csv").
        option("header", "true").
        option("Delimiter", ",").
        load("c:/spark/bin/data/" + csvFile)

    var final_data_rdd = final_data.rdd

    var columns = final_data.columns
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
    var fcstNo = columns.indexOf("fcst")
    var time_seriesNo = columns.indexOf("time_series")

    var final_data_result = final_data_rdd.map(x => {

      var newPromotionYN = 0

      var year = x.getString(yearweekNo).substring(0,4)
      var week = x.getString(yearweekNo).substring(4)

      if(x.getString(promotionYN) == "Y"){
        newPromotionYN = 1
      }

      (
        x.getString(regionSeg1No),
        x.getString(regionSeg3No),
        x.getString(salesidNo),
        x.getString(productgroupNo),
        x.getString(itemNo),
        x.getString(yearweekNo),
        year,
        week,
        x.getString(map_priceNo),
        x.getString(irNo),
        x.getString(pmapNo),
        x.getString(pmap10No),
        x.getString(pro_percentNo),
        x.getString(qtyNo),
        newPromotionYN,
        x.getString(moving_avgNo),
        x.getString(seasonalityNo),
        x.getString(fcstNo),
        x.getString(time_seriesNo)
      )
    })

    var finalDF = final_data_result.toDF("regionseg", "regionseg3","salesid","productgroup","item","yearweek","year","week","map_price","ir","pmap","pmap10","pro_percent","qty","promotionNY","moving_avg", "seasonality", "fcst", "time_series")

    finalDF.
      coalesce(1).
      write.format("csv").
      mode("overwrite").
      option("header", "true").
      save("c:/final_data.csv")
  }
}

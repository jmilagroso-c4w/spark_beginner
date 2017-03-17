package com.cloud4wi.spark

/* Spark Libraries */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/* Typesafe config Library*/
import com.typesafe.config.ConfigFactory

object Demo {
  val config = ConfigFactory.load()

  def main(args: Array[String]) {
    val logFile = "src/main/resources/data/1M.txt"

    val conf = new SparkConf()
      .setAppName("Simple Apache Spark Application")

    if (config.getBoolean("set.master.and.exec.memory")) {
      conf.setMaster(config.getString("spark.master"))
          .set("spark.executor.memory", config.getString("spark.executor.memory"))
    }

    val sc = new SparkContext(conf)
    val inputRDD = sc.textFile(logFile, 2)

    // Filter
    val filter1 = "1234"
    val filter2 = "letmein"
    val numAs = inputRDD.filter(line => line.contains(filter1)).count()
    val numBs = inputRDD.filter(line => line.contains(filter2)).count()
    println(s"Filter results. Lines with $filter1: $numAs, Lines with $filter2: $numBs")
    // Filter results. Lines with 1234: 7441, Lines with letmein: 51

    // Map
    val pairs = inputRDD.map(s => (s, 1))
    val counts = pairs.reduceByKey((a, b) => a + b).count()
    println(s"Map results. Count: $counts")
    // Map results. Count: 999999
    sc.stop()
  }
}
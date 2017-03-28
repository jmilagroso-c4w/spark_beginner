package com.cloud4wi.spark

/* Spark Libraries */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
/* Typesafe config Library*/
import com.typesafe.config.ConfigFactory

import com.cloud4wi.spark.Test._

object Demo {
  val config = ConfigFactory.load()

  def sparkContext(): SparkContext ={
    val conf = new SparkConf()
      .setAppName("Simple Apache Spark Application")

    if (config.getBoolean("set.master.and.exec.memory")) {
      conf.setMaster(config.getString("spark.master"))
        .set("spark.executor.memory", config.getString("spark.executor.memory"))
    }

    new SparkContext(conf)
  }

  def logFile(): String ={
    "src/main/resources/data/1M.txt"
  }


  def main(args: Array[String]) {

    // Map sampleMap
    // this.sampleMap()

    // FlatMap sampleFlatMap
    this.sampleFlatMap()

    // Filter sampleFilterFromLogFile
    // this.sampleFilterFromLogFile()

    // Filter sampleFilter
    // this.sampleFilter()

    // Foreach Partition
    // this.sampleForeachPartition()

    // Aggregate
    // this.sampleAggregate()

    // Aggregate By Key
    // this.sampleAggregateByKey()
  }

  /*
  Evaluates a boolean function for each data item of the RDD and puts the items for which the function returned true into the resulting RDD.
   */
  def sampleFilterFromLogFile(): Unit ={
    val sc = this.sparkContext()

    val inputRDD = sc.textFile(this.logFile(), 2)

    val filter1 = "1234"
    val filter2 = "letmein"
    val numAs = inputRDD.filter(line => line.contains(filter1)).count()
    val numBs = inputRDD.filter(line => line.contains(filter2)).count()
    println(s"Filter results. Lines with $filter1: $numAs, Lines with $filter2: $numBs")

    sc.stop
  }

  /*
  Evaluates a boolean function for each data item of the RDD and puts the items for which the function returned true into the resulting RDD.
   */
  def sampleFilter(): Unit ={
    val sc = this.sparkContext()

    val a = sc.parallelize(1 to 10, 3)
    val b = a.filter(_ % 2 == 0) // All even

    // Sorted
    b.collect().foreach( b => {
      println("collect():" + b)
    })

    // Unsorted
    /*b.foreachPartition(
      _ foreach( n => {
        println("foreachPartition():" + n)
      })
    )*/

    sc.stop
  }

  /*
Sample ForeachPartition from Log file
 */
  def sampleForeachPartition(): Unit ={
    val sc = this.sparkContext()

    val inputRDD = sc.textFile(this.logFile(), 2)

   inputRDD.foreachPartition(
     _ foreach { n =>

      println(n)

     } // _ foreach { n =>
   )

    sc.stop
  }

  /*
  The aggregate function allows the user to apply two different reduce functions to the RDD.
  The first reduce function is applied within each partition to reduce the data within each partition into a single result.
  The second reduce function is used to combine the different reduced results of all partitions together to arrive at one final result.
  The ability to have two separate reduce functions for intra partition versus across partition reducing adds a lot of flexibility.
  For example the first reduce function can be the max function and the second one can be the sum function.
  The user also specifies an initial value. Here are some important facts.
   */
  def sampleAggregate(): Unit = {
    val sc = this.sparkContext()
    val z = sc.parallelize(List(1,2,3,4,5,6), 2)

    // This example returns 16 since the initial value is 1
    // reduce of partition 0 will be max(1, 1, 2, 3) = 3     // reduce of partition 0 will be min(1, 1, 2, 3) = 1
    // reduce of partition 1 will be max(1, 4, 5, 6) = 6     // reduce of partition 1 will be min(1, 4, 5, 6) = 1
    // final reduce across partitions will be 1 + 3 + 6 = 10 // final reduce across partitions will be 1 + 1 + 1 = 3
    // note the final reduce include the initial value
    z.aggregate(1)(math.min(_, _), _ + _) is 3
    z.aggregate(1)(math.max(_, _), _ + _) is 10
    z.aggregate(0)(_ + _, _ + _) is 21

    sc.stop()
  }

  /*
  Works like the aggregate function except the aggregation is applied to the values with the same key.
  Also unlike the aggregate function the initial value is not applied to the second reduce.
   */
  def sampleAggregateByKey(): Unit = {
    val sc = this.sparkContext()

    val pairRDD = sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    // Breakdown
    pairRDD.foreachPartition(
      _ foreach( n => {
        println(n._1 + " => " + n._2)
      })
    )

    // Aggregated by key breakdown
    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).foreachPartition(
      _ foreach( n => {
        // Array((dog,12), (cat,17), (mouse,6))
        println(n._1 + " => " + n._2)
      })
    )

    sc.stop()
  }

  /*
  Applies a transformation function on each item of the RDD and returns the result as a new RDD.
   */
  def sampleMap(): Unit = {
    val sc = this.sparkContext()

    // List of data with 3 partitions
    val a = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    // Map each data with its length
    val b = a.map(_.length)
    // Zip. Joins two RDDs by combining the i-th of either partition with each other.
    // The resulting RDD will consist of two-component tuples which are interpreted as key-value pairs by the
    // methods provided by the PairRDDFunctions extension.
    val c = a.zip(b) //

    c.foreachPartition(
      _ foreach( n => {
        println(n._1 + "=>" + n._2)
      })
    )

    sc.stop()
  }

  /*
  Similar to map, but allows emitting more than one item in the map function.
   */
  def sampleFlatMap(): Unit = {
    val sc = this.sparkContext()

    val a = sc.parallelize(List(1, 2, 3), 2)
    a.flatMap(1 to _).foreachPartition(
      _ foreach( n => {
        println(n)
      })
    )


    sc.stop()
  }

}
/**
 * Created by kalit_000 on 21/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import  org.apache.spark.graphx._
import  org.apache.spark.rdd.RDD


object SparkTest {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkGraphFromFile").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)


    println("Hi kali")


  }


}



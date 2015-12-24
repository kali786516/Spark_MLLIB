
/**
 * Created by kalit_000 on 21/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.SQLContext

object SparkDonorsChooseMLIB {

  def main (args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkMLLIB").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)

    /*source file*/
    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_safari_mllib\\opendata_projects.csv")

    /*Remove headers in CSV*/
    val file_head=file.first()
    val file_no_header=file.filter(x => x != file_head)
    val rdd_csv=file_no_header.map(x => x.split(","))

   /*lenght of headers vs lenght of data*/
    //Data Quality
    rdd_csv.first().foreach(println)

    val headers_length=file_head.split(",").length
    val bad_data=rdd_csv.filter(x => x.length != headers_length)
    println("Bad Data Count:-%s".format(bad_data.count()))

    //Good Data
    val gooddatardd=rdd_csv.filter(x => x.length == headers_length)
    println("Good Data Count:-%s".format(gooddatardd.count()))
    println("Total Data Count:-%s".format(rdd_csv.count()))

   println(file_head)

    println(gooddatardd.map(x => (x(2))).countApproxDistinct())

   import sqlContext.implicits._














  }


}

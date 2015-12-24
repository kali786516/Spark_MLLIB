/**
 * Created by kalit_000 on 24/12/2015.
 */

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.util.StatCounter

case class destiantion (airportid:String,arrdealy:Double)

case class origing (airportid:String,depdealy:Double)

object SaprkFlightDataExample {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark_flightData_example").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlcontest=new SQLContext(sc)

    val file=sc.textFile("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_safari_mllib\\463015547_T_ONTIME.csv")

    val header=file.first().toString

    val header_list=header.split(",")

    val airline_no_header=file.filter(x => x != header)

    val airline_no_quote=airline_no_header.map(x => x.replace("\'","").replace("\"","").split(","))

    val destination_rdd=airline_no_quote.map(x => (x(4),x(7))).map {case (dest_airport_id,arr_dealy) => (dest_airport_id, if (arr_dealy.length > 0) arr_dealy.toFloat else 0)}

    /* if in case if we need full set of columns
    airline_no_quote.map{ case (dest_airport_id,arr_delay) => (dest_airport_id,arr_delay)}*/
    val origin_rdd=airline_no_quote.map(x => (x(3),x(5))).map{ case (origin_airport_id,dep_delay) => (origin_airport_id,if (dep_delay.length > 0) dep_delay.toFloat else 0 )}

    import sqlcontest.implicits._

    destination_rdd.map(x => destiantion(x._1,x._2)).toDF().registerTempTable("destination")
    origin_rdd.map(x => origing(x._1,x._2)).toDF().registerTempTable("origin")

    println("top 5 best aiport to arrive on time")
    sqlcontest.sql("select airportid,(sum(arrdealy)/count(1)) as mean_of_arr_delay  from destination  group by airportid order by (sum(arrdealy)/count(1)) asc").take(5).foreach(println)

    println("top 5 best aiport to depart on time")
    sqlcontest.sql("select airportid,(sum(depdealy)/count(1)) as mean_of_dep_delay  from origin  group by airportid order by (sum(depdealy)/count(1)) asc").take(5).foreach(println)

    println("top 5 worst aiport to arrive on time")
    sqlcontest.sql("select airportid,(sum(arrdealy)/count(1)) as mean_of_arr_delay  from destination  group by airportid order by (sum(arrdealy)/count(1)) desc").take(5).foreach(println)

    println("top 5 worst aiport to depart on time")
    sqlcontest.sql("select airportid,(sum(depdealy)/count(1)) as mean_of_dep_delay  from origin  group by airportid order by (sum(depdealy)/count(1)) desc").take(5).foreach(println)


  }
}

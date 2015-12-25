/**
 * Created by kalit_000 on 24/12/2015.
 */



import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

object orange {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("local[*]").setAppName("orange").set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    val sqlcontext=new SQLContext(sc)

    val df = sqlcontext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .option("delimiter", "\t")
      .load("C:\\Users\\kalit_000\\Desktop\\typesafe\\spark_safari_mllib\\bulk_data\\churn\\small_combined.tsv")


    val df2=df.na.fill(0)


    df2.registerTempTable("orange")
    df2.printSchema()


  sqlcontext.sql("" +
      "select Var1, Var2, Var3, Var4, Var5, Var6, Var7, Var8, Var9, Var10, " +
      "Var11, Var12, Var13, Var14, Var15, Var16, Var17, Var18, Var19, Var20, " +
      "Var21, Var22, Var23, Var24, Var25, Var26, Var27, Var28, Var29, Var30, " +
      "Var31, Var32, Var33, Var34, Var35, Var36, Var37, Var38, Var39, Var40, " +
      "Var41, Var42, Var43, Var44, Var45, Var46, Var47, Var48, Var49, Var50, " +
      "Var51, Var52, Var53, Var54, Var55, Var56, Var57, Var58, Var59, Var60, " +
      "Var61, Var62, Var63, Var64, Var65, Var66, Var67, Var68, Var69, Var70, " +
      "Var71, Var72, Var73, Var74, Var75, Var76, Var77, Var78, Var79, Var80, " +
      "Var81, Var82, Var83, Var84, Var85, Var86, Var87, Var88, Var89, Var90, " +
      "Var91, Var92, Var93, Var94, Var95, Var96, Var97, Var98, Var99, Var100, " +
      "Var101, Var102, Var103, Var104, Var105, Var106, Var107, Var108, Var109," +
      " Var110, Var111, Var112, Var113, Var114, Var115, Var116, Var117, Var118,Var119, Var120," +
      " Var121, Var122, Var123, Var124, Var125, Var126, Var127, Var128, Var129, Var130," +
      " Var131, Var132, Var133, Var134, Var135, Var136, Var137, Var138, Var139, Var140, " +
      "Var141, Var142, Var143, Var144, Var145, Var146, Var147, Var148, Var149, Var150, " +
      "Var151, Var152, Var153, Var154, Var155, Var156, Var157, Var158, Var159, Var160, " +
      "Var161, Var162, Var163, Var164, Var165, Var166, Var167, Var168, Var169," +
      " Var170, Var171, Var172, Var173, Var174, Var175, Var176, Var177, Var178, Var179, Var180, " +
      "Var181, Var182, Var183, Var184, Var185, Var186, Var187, Var188, Var189, Var190 from orange limit 1").foreach(println)






  }
}

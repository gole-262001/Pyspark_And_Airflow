import org.apache.commons.lang3.ObjectUtils.median
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Solution {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MarketingAnalysisproject")
      .master("local[1]")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    // 1. Load data and create Spark data frame

    val data = spark.read.option("header", "true")
      .csv("Inputfile/InputFile.csv")
       data.show()

    // 2.a) Give marketing success rate. (No. of people subscribed / total no. of entries) (Spark SQL)
    // b) Give marketing failure rate

    data.createOrReplaceTempView("mytable")
    val successDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'success'")
    val failureDF = spark.sql("SELECT count(*) AS successCount FROM mytable WHERE poutcome = 'failure'")
    successDF.show()

    val numDF = spark.sql("SELECT count(*) AS totalCount FROM mytable")

    numDF.show()
    val successCount = successDF.first().getLong(0)
    val failureRate = failureDF.first().getLong(0)
    println(successCount)
    val totalCount = numDF.first().getLong(0)
    println(totalCount)
    val MarketingSuccessRate = successCount.toDouble / totalCount
    val  marketingFailureRate = failureRate.toDouble / totalCount

    println("Marketing Success Rate: " + MarketingSuccessRate*100)
    println("Marketing Failure Rate: " + marketingFailureRate*100)

    // 3. Maximum, Mean, and Minimum age of average targeted customer


    println("avg: " +data.select(avg("age")).collect()(0)(0))
    println("min: " +data.select(min("age")).collect()(0)(0))
    println("max: " +data.select(max("age")).collect()(0)(0))

   // 4. Check quality of customers by checking average balance , median balance of customers

    println("avgerage balance : " +data.select(avg("balance")).collect()(0)(0))

    val medianValue = data.select(median("balance")).collect()(0)(0)
    println(s"The median price is: $medianValue")

   // 5. Check if age matters in marketing subscription for deposit

    val age  = spark.sql("select age , count(*) as number from mytable where y = 'yes' group by age order by number desc").show()

   //  6. Check if marital status mattered for subscription to deposit.
    val marital = spark.sql("select marital, count(*) as number from mytable where y='yes' group by marital order by number desc ").show()

    //    7. Check if age and marital status together mattered for subscription to deposit scheme

    val age_marital = spark.sql("select age, marital, count(*) as number from mytable where y='yes' group by age,marital order by number desc ").show()


    //    8. Do feature engineering for column —age and find right age effect on campaign



    //     .9 Check if age matters in marketing subscription for deposit

    val Newdata = data.withColumnRenamed("default", "default_flag")


    Newdata.show();
       Newdata.write
      .format("org.apache.spark.sql.cassandra")
      .option("table", "marketinganalysis")
      .option("keyspace", "my_keyspace")
      .mode("append")
      .save()

    spark.stop()


  }

}

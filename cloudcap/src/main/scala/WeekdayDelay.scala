package cloudcap


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

//1.3
object WeekdayDelay {


  val onTimeSchema = StructType(
    Array(StructField("FlightDate", DateType),
      StructField("UniqueCarrier", StringType),
      StructField("FlightNum", StringType),
      StructField("Origin", StringType),
      StructField("Dest", StringType),
      StructField("DepTime", IntegerType),
      StructField("DepDelay", DoubleType),
      StructField("ArrTime", IntegerType),
      StructField("ArrDelay", DoubleType),
      StructField("Cancelled", DoubleType),
      StructField("Diverted", DoubleType)
    )
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Top10Airports <input directory>")
      System.exit(1)
    }

    val spark = SparkSession.builder
      .master("yarn")
      .appName("cloudcap")
      .getOrCreate()

    import spark.implicits._

    val onTimeDF = spark.read
      .option("header", "false")
      .schema(onTimeSchema)
      .csv(args(0))

    // Filter and map equivalent
    val filtered = onTimeDF
      .filter(col("Cancelled").equalTo("0.0") && col("Diverted").equalTo("0.0"))
        .select(col("FlightDate"), col("ArrDelay")).rdd.map(r => (DayOfWeek(r.getDate(0).toString), r.getDouble(1)))
        .toDF("FlightDate", "ArrDelay")

    // Reduce equivalent <=> agg function
    val topDays = filtered.groupBy("FlightDate").agg(avg("ArrDelay") as "AvgDelay").orderBy($"AvgDelay".asc)

    // Show results
    topDays.show()
    topDays.repartition(1).sortWithinPartitions($"AvgDelay".asc)
      .write.format("com.databricks.spark.csv").option("header", "true").save(args(1))

    spark.stop()
  }

  //Helper function to get day of week
  def DayOfWeek(date: String): String = {
    val df = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    LocalDate.parse(date, df).getDayOfWeek.name()
  }

}


package cloudcap

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//1.2
object Top10AirlinesByArrivalTime {


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
    if (args.length < 2) {
      System.err.println("Usage: Top10Airports <input directory> <output directory>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
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
        .select(col("UniqueCarrier"), col("ArrDelay"))

    // Reduce equivalent <=> agg function
    val top10Airlines = filtered.groupBy("UniqueCarrier")
      .agg(avg("ArrDelay") as "AvgDelay")
      .orderBy($"AvgDelay".asc)

    top10Airlines.coalesce(1).sortWithinPartitions($"AvgDelay")
      .write.format("com.databricks.spark.csv").option("header", "true").save(args(1))

    spark.stop()
  }

}

package cloudcapcas


import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


//2.1
object Top10AirlinesFromX {


  val onTimeSchema = StructType(
    Array(StructField("flightdate", DateType),
      StructField("uniquecarrier", StringType),
      StructField("flightnum", StringType),
      StructField("origin", StringType),
      StructField("dest", StringType),
      StructField("deptime", IntegerType),
      StructField("depdelay", DoubleType),
      StructField("arrtime", IntegerType),
      StructField("arrdelay", DoubleType),
      StructField("cancelled", DoubleType),
      StructField("diverted", DoubleType)
    )
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: Top10Airports <input directory>")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .master("yarn")
      .config("spark.cassandra.connection.host", "ip-172-31-29-143.ec2.internal")
      .config("spark.cassandra.connection.port", "9042")
      .appName("cloudcapcas")
      .getOrCreate()

    import spark.implicits._

    val onTimeDF = spark.read
      .option("header", "false")
      .schema(onTimeSchema)
      .csv(args(0))

    val filtered = onTimeDF
      .filter(col("cancelled").equalTo("0.0") && col("origin").isin("CMI", "BWI", "MIA", "LAX", "IAH", "SFO"))
        .select(col("uniquecarrier"), col("origin"), col("depdelay"))

    val top10Airlines = filtered.groupBy("origin","uniquecarrier")
      .agg(avg("depdelay") as "avgdelay")
      .orderBy($"origin", $"avgdelay".asc)

    top10Airlines.write.format("org.apache.spark.sql.cassandra")
      .options(
        Map("table" -> "top10airlinesfromx",
          "keyspace" -> "cloudcap"
        )
      ).mode(SaveMode.Overwrite)
      .save()

    //top10Airlines.repartition(1).sortWithinPartitions($"Origin", $"AvgDelay".asc)
    // .write.format("com.databricks.spark.csv").option("header", "true").save(args(1))
    spark.stop()
  }

}

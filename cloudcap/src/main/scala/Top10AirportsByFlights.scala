package cloudcap

import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel


//1.1
object Top10AirportsByFlights {


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
      System.err.println("Usage: Top10Airlines <input directory> <output directory>")
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

    onTimeDF.createOrReplaceTempView("ontime")

    // Filter and map equivalent
    val topOriginAir = spark.sql("select distinct(Origin) as airport, count(Origin) as Count from ontime group by Origin order by Count desc")
    val topDestAir = spark.sql("select distinct(Dest)as airport, count(Dest) as Count from ontime group by Dest order by Count desc")

    val allTop = topDestAir.union(topOriginAir)

    // Reduce equivalent <=> agg function
    val fin = allTop.groupBy("airport").agg(sum("Count") as "Sum").orderBy($"Sum".desc)

    fin.repartition(1).sortWithinPartitions($"Sum".desc)
      .write.format("com.databricks.spark.csv").option("header", "true").save(args(1))

    spark.stop()

  }

}

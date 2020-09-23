package org.example.moviereviews.streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object CalcMovieNamesCreditedPeople {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local").appName("CalcMovieNamesCreditedPeople").getOrCreate()

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val titlePrincipalsPath = "data/input/input_title_principals/"
    val titleBasicsPath = "data/input/input_title_basics/"
    val topMoviesPath = "data/output/top_movies"
    val waterMarkDelay = "5 seconds"
    val triggerTimeInterval = "5 seconds"
    val checkPointPath ="checkpoint/movienanes_creditedpeople"


    val principalsDF = spark.readStream.format("csv").option("sep", "\t").option("header", true)
      .schema(MovieDataSchemas.principalSchema).load(titlePrincipalsPath)
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)

    val basicsDF = spark.readStream.format("csv").option("sep", "\t").option("header", true).schema(MovieDataSchemas.basicsSchema).load(titleBasicsPath)
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)

    //adding '/*' to the path, so it can reads files from nested folders.
    val topMoviesDF = spark.readStream.format("csv").option("header", true).schema(MovieDataSchemas.moviesSchema).load(topMoviesPath+"/*")
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)


    val joinedDS = principalsDF.join(topMoviesDF, "tconst")
      .join(basicsDF, "nconst").select("tconst", "titleId", "title", "primaryName", "primaryProfession", "knownForTitles").dropDuplicates()

    val joinedDSQuery = joinedDS
      .writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .option("checkpointLocation", checkPointPath)
      .trigger(Trigger.ProcessingTime(triggerTimeInterval))
      .start()

    joinedDSQuery.awaitTermination()

  }
}

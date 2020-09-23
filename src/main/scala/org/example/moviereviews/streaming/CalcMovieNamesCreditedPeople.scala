package org.example.moviereviews.streaming

import java.io.File
import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

object CalcMovieNamesCreditedPeople {
  def main(args: Array[String]): Unit = {
    //limiting logs to warning.
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("moviereviews").getConfig("movienanes_creditedpeople")
    val spark = SparkSession.builder().master(config.getString("master")).appName("CalcMovieNamesCreditedPeople").getOrCreate()

    startStreaming(spark, config)
  }

  def startStreaming(spark : SparkSession, config : Config): Unit = {

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val titlePrincipalsPath = config.getString("titlePrincipalsPath")
    val titleBasicsPath = config.getString("titleBasicsPath")
    val topMoviesPath = config.getString("topMoviesPath")
    val waterMarkDelay = config.getString("waterMarkDelay")
    val triggerTimeInterval = config.getString("triggerTimeInterval")
    val checkPointPath = config.getString("checkPointPath")

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

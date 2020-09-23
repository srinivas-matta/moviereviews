package org.example.moviereviews.streaming

import java.io.File
import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{desc, lit}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf

object CalcTopMovies {

  def main(args: Array[String]): Unit = {
    //limiting logs to warning.
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("moviereviews").getConfig("top_movies")
    val spark = SparkSession.builder().master(config.getString("master")).appName("CalcTopMovies").getOrCreate()

    startStreaming(spark, config)
  }

  def startStreaming(spark: SparkSession, config: Config): Unit = {

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val titleRatingsPath = config.getString("titleRatingsPath")
    val titleAkasPath = config.getString("titleAkasPath")
    val averageVotesPath = config.getString("averageVotesPath")
    val topMoviesPath = config.getString("topMoviesPath")
    val waterMarkDelay = config.getString("waterMarkDelay")
    val triggerTimeInterval = config.getString("triggerTimeInterval")
    val checkPointPath = config.getString("checkPointPath")
    val numberOfTopMovies = config.getInt("numberOfTopMovies")

    val titleRatingsDF = spark.readStream.format("csv").option("sep", "\t").option("header", true).schema(MovieDataSchemas.titleRatingsSchema).load(titleRatingsPath).
      withColumn("dummy", lit("1"))
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)

    val titleAkasDF = spark.readStream.format("csv").option("sep", "\t").option("header", true).schema(MovieDataSchemas.akasSchema).load(titleAkasPath)
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)

    val averageNumberOfVotesDF = spark.readStream.format("csv").option("header", true).schema(MovieDataSchemas.averageNumberOfVotesSchema).
      load(averageVotesPath + "/*") //adding '/*' to the path so it can reads files from nested folders.
      .withColumn("dummy", lit("1")) //adding dummy column for joining other data frames which doesn't have common column
      .withWatermark("timestamp", waterMarkDelay)


    val calcAvgRating = (numVotes: Int, averageRating: Double, averageNumberOfVotes: Double) => {
      (numVotes / (averageNumberOfVotes * averageRating))
    }

    val calcAvgRatingUDF = udf(calcAvgRating)

    val calcTopMovies = (present_df: DataFrame, batchId: Long) => {
      if (!present_df.isEmpty) {
        val topMoviesDF = present_df.orderBy(desc("ratingRank")).cache()
        val top10Movies = topMoviesDF.groupBy("titleId").count().select("titleId").limit(numberOfTopMovies)
        print("top 10 movies")
        top10Movies.show()

        print("top 10 movies with different titles")
        val topMoviesDF_ = topMoviesDF.join(top10Movies, "titleId").select("tconst", "averageRating", "numVotes", "ratingRank", "titleId", "title", "region").dropDuplicates()
        topMoviesDF_.show()
        //writing results to files.
        topMoviesDF_.coalesce(1).write.option("header", "true").csv(topMoviesPath + "/" + System.currentTimeMillis())
      }
    }

    val joinedDS = averageNumberOfVotesDF.join(titleRatingsDF, "dummy").
      withColumn("ratingRank", calcAvgRatingUDF(titleRatingsDF("numVotes"), titleRatingsDF("averageRating"), averageNumberOfVotesDF("averageNumberOfVotes")))
      .drop("dummy")
      .dropDuplicates()
      .join(titleAkasDF, titleRatingsDF.col("tconst") === titleAkasDF.col("titleId"))

    val joinedStream = joinedDS.writeStream
      .outputMode(OutputMode.Append())
      .queryName("TopMovies")
      .option("checkpointLocation", checkPointPath)
      .foreachBatch {
        calcTopMovies
      }
      .trigger(Trigger.ProcessingTime(triggerTimeInterval))
      .start()

    joinedStream.awaitTermination()

  }
}

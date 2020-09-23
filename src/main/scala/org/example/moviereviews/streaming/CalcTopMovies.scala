package org.example.moviereviews.streaming

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{desc, lit}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object CalcTopMovies {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local").appName("CalcTopMovies").getOrCreate()

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val titleRatingsPath = "data/input/input_title_ratings"
    val titleAkasPath = "data/input/input_title_akas"
    val averageVotesPath = "data/output/average_number_of_votes"
    val topMoviesPath = "data/output/top_movies"
    val waterMarkDelay = "5 seconds"
    val triggerTimeInterval = "5 seconds"
    val checkPointPath = "checkpoint/topmovies"
    val numberOfTopMovies = 10




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
      // .format("console")
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

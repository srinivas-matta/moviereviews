package org.example.moviereviews.batch

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object MovieReviews {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val spark = SparkSession.builder().master("local").appName("batchMovieReviews").getOrCreate()

    import org.apache.spark.sql.functions._

    val titleRatingsDF = spark.read.option("sep", "\t").option("header", true).option("inferSchema", true).csv("data/input/input_title_ratings")
    val titleAkasDF = spark.read.option("sep", "\t").option("header", true).option("inferSchema", true).csv("data/input/input_title_akas")
    val principalsDF = spark.read.option("sep", "\t").option("header", true).option("inferSchema", true).csv("data/input/input_title_principals")
    val basicsDF = spark.read.option("sep", "\t").option("header", true).option("inferSchema", true).csv("data/input/input_title_basics")
    val numberOfTopMovies = 10

    val titleRatings_500_DF = titleRatingsDF.filter("numVotes > 500").orderBy(desc("averageRating"))

    val averageNumberOfVotesDF = titleRatingsDF.agg(avg("numVotes").as("averageNumberOfVotes"))

    val averageNumberOfVotesValue = averageNumberOfVotesDF.limit(1).collect()(0)(0).asInstanceOf[Double]

    val calcAvgRating = (numVotes: Int, averageRating: Double, averageNumberOfVotes: Double) => {
      (numVotes / (averageNumberOfVotes * averageRating))
    }

    import org.apache.spark.sql.functions.udf
    val calcAvgRatingUDF = udf(calcAvgRating)

    val topMoviesDF = titleRatings_500_DF.withColumn("averageRatingRank", calcAvgRatingUDF(titleRatings_500_DF("numVotes"), titleRatings_500_DF("averageRating"), lit(averageNumberOfVotesValue))).
      orderBy(desc("averageRatingRank")).limit(numberOfTopMovies)

    println("top movies..")
    topMoviesDF.show()

    val topMoviesWithTitlesDF = topMoviesDF.join(titleAkasDF, topMoviesDF.col("tconst") === titleAkasDF.col("titleId")).
      groupBy("tconst","title", "region", "averageRatingRank").count().
      orderBy(desc("averageRatingRank"))

    println("top movies with different titles")
    topMoviesWithTitlesDF.show()

    println("Top movies with Credited People (primaryName) ")
    principalsDF.join(topMoviesWithTitlesDF, "tconst")
      .join(basicsDF, "nconst").groupBy("primaryName").count().orderBy(desc("count")).
      show()
  }
}

package org.example.moviereviews.streaming

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object MovieDataSchemas {

  val titleRatingsSchema = StructType(
    List(
      StructField("tconst", StringType, true),
      StructField("averageRating", DoubleType, true),
      StructField("numVotes", IntegerType, true)
    )
  )

  val akasSchema = StructType(
    List(
      StructField("titleId", StringType, true),
      StructField("ordering", IntegerType, true),
      StructField("title", StringType, true),
      StructField("region", StringType, true),
      StructField("language", StringType, true),
      StructField("types", StringType, true),
      StructField("attributes", StringType, true),
      StructField("isOriginalTitle", StringType, true)
    )
  )

  val averageNumberOfVotesSchema = StructType(
    List(
      StructField("averageNumberOfVotes", DoubleType, true),
      StructField("timestamp", TimestampType, true)
    )
  )

  val principalSchema = StructType(
    List(
      StructField("tconst", StringType, true),
      StructField("ordering", IntegerType, true),
      StructField("nconst", StringType, true),
      StructField("category", StringType, true),
      StructField("job", StringType, true),
      StructField("characters", StringType, true)
     )
  )

  val basicsSchema = StructType(
    List(
      StructField("nconst", StringType, true),
      StructField("primaryName", StringType, true),
      StructField("birthYear", StringType, true),
      StructField("deathYear", StringType, true),
      StructField("primaryProfession", StringType, true),
      StructField("knownForTitles", StringType, true)
    )
  )

  val moviesSchema = StructType(
    List(
      StructField("tconst", StringType, true),
      StructField("averageRating", DoubleType, true),
      StructField("numVotes", IntegerType, true),
      StructField("ratingRank", DoubleType, true),
      StructField("titleId", StringType, true),
      StructField("title", StringType, true),
      StructField("region", StringType, true)
    )
  )
}

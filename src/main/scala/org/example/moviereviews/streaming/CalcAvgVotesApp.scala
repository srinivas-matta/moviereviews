package org.example.moviereviews.streaming

import java.io.File
import java.sql.Timestamp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, lit}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object CalcAvgVotesApp {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val config = ConfigFactory.parseFile(new File("application.conf")).getConfig("moviereviews.avgvotes")
    val spark = SparkSession.builder().master("local").appName("CalcAvgVotesApp").getOrCreate()
    print("config :"+config)
    startStreaming(spark, config)
  }

  def startStreaming(spark: SparkSession, config: Config): Unit = {

    spark.sql("set spark.sql.streaming.schemaInference=true")

    val titleRatingsFilesPath = config.getString("titleRatingsFilesPath")
    val averageVotesPath = config.getString("averageVotesPath")
    val averageNumberOfVotesColumnName = config.getString("averageNumberOfVotesColumnName")
    val waterMarkDelay = config.getString("waterMarkDelay")
    val triggerTimeInterval = config.getString("triggerTimeInterval")
    val checkPointPath = config.getString("checkPointPath")

    val df_title_ratings = spark.readStream.format("csv").option("sep", "\t").option("header", true).schema(MovieDataSchemas.titleRatingsSchema).load(titleRatingsFilesPath)

    val averageNumberOfVotesDF = df_title_ratings.agg(avg("numVotes").as("averageNumberOfVotes"))
      .withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
      .withWatermark("timestamp", waterMarkDelay)

    val saveAverageNumberOfVotes = (present_df: DataFrame, batchId: Long) => {
      println(s"Batch ID: $batchId")
      present_df.show()
      val file: File = new File(averageVotesPath)
      if (!present_df.isEmpty) { // saving only if data frame contains records.
        if (file.isDirectory() && file.list().length > 0) {
          //merging previous data with current data to get updated average.
          val previous_df = spark.read.format("csv").option("header", true).option("inferSchema", true).load(averageVotesPath + "/*")
          val merged_df = previous_df.union(present_df).agg(avg(averageNumberOfVotesColumnName).as(averageNumberOfVotesColumnName)).withColumn("timestamp", lit(new Timestamp(System.currentTimeMillis())))
          //reducing number of partitions to 1 so it will write only one file.
          merged_df.coalesce(1).write.mode(SaveMode.Overwrite).option("header", "true").csv(averageVotesPath + "/" + System.currentTimeMillis())
        } else {
          present_df.coalesce(1).write.option("header", "true").csv(averageVotesPath + "/" + System.currentTimeMillis())
        }
      }
    }

    val queryAverageNumberOfVotesResults = averageNumberOfVotesDF
      .writeStream
      .outputMode("complete")
      .foreachBatch {
        saveAverageNumberOfVotes
      }
      .option("checkpointLocation", checkPointPath)
      .trigger(Trigger.ProcessingTime(triggerTimeInterval))
      .start()

    queryAverageNumberOfVotesResults.awaitTermination()

  }
}

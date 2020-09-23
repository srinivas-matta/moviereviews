package org.example.moviereviews.datagenerator

import java.nio.file.{Files, Paths, StandardCopyOption}

object MovieReviewsDataGenerator extends App {
  print("Generating data for movie reviews.")

  val source_title_ratings = "data/source/title_ratings.small.tsv"
  val source_principals = "data/source/title.principals.small.tsv"
  val source_basics = "data/source/title.basics.small.tsv"
  val source_akas = "data/source/title.akas.small.tsv"


  def copyRenameFile(source: String, destination: String): Unit = {
    val path = Files.copy(
      Paths.get(source),
      Paths.get(destination),
      StandardCopyOption.REPLACE_EXISTING
    )

  }

  val copyFilesThread  = new Thread(){
    override def run(): Unit = {
      for(i <- 1 to 10) {
        copyRenameFile(source_title_ratings, "data/input/input_title_ratings/"+"title_ratings.small_"+System.currentTimeMillis()+"_.tsv" )
        copyRenameFile(source_principals, "data/input/input_title_principals/"+"title_principals.small_"+System.currentTimeMillis()+"_.tsv" )
        copyRenameFile(source_basics, "data/input/input_title_basics/"+"title_basics.small_"+System.currentTimeMillis()+"_.tsv" )
        copyRenameFile(source_akas, "data/input/input_title_akas/"+"title_akas.small_"+System.currentTimeMillis()+"_.tsv" )
        Thread.sleep(10000)
      }
    }
  }

  copyFilesThread.start()



}

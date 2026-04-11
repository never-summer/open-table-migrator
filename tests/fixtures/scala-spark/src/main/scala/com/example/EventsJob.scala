package com.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object EventsJob {
  def loadEvents(spark: SparkSession, path: String): DataFrame =
    spark.read.parquet(path)

  def saveEvents(df: DataFrame, path: String): Unit =
    df.write.mode("overwrite").parquet(path)

  def countByStatus(df: DataFrame): DataFrame =
    df.groupBy("status").count()
}

package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class EventsJob {
    public static Dataset<Row> loadEvents(SparkSession spark, String path) {
        return spark.read().parquet(path);
    }

    public static void saveEvents(Dataset<Row> df, String path) {
        df.write().mode("overwrite").parquet(path);
    }

    public static Dataset<Row> countByStatus(Dataset<Row> df) {
        return df.groupBy("status").count();
    }
}

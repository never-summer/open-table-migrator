package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveEtl {
    public static void createHiveTable(SparkSession spark) {
        spark.sql("CREATE TABLE IF NOT EXISTS events (id BIGINT, status STRING, value DOUBLE) STORED AS PARQUET");
    }

    public static Dataset<Row> readHive(SparkSession spark) {
        return spark.sql("SELECT * FROM events");
    }

    public static void writeHive(Dataset<Row> df) {
        df.write().mode("overwrite").saveAsTable("events");
    }
}

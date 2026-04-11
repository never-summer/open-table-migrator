from pyspark.sql import SparkSession, DataFrame


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("etl").getOrCreate()


def load_events(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)


def save_events(df: DataFrame, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def count_by_status(df: DataFrame) -> DataFrame:
    return df.groupBy("status").count()

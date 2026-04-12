from skills.open_table_migrator.transformers.pyspark import transform_pyspark_file


def test_transforms_spark_read_parquet():
    source = 'df = spark.read.parquet("s3://bucket/events/")\n'
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert "spark.read.parquet" not in result
    assert 'spark.read.format("iceberg")' in result or 'spark.table(' in result


def test_transforms_spark_write_parquet():
    source = 'df.write.mode("overwrite").parquet("s3://bucket/out/")\n'
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert ".parquet(" not in result
    assert 'format("iceberg")' in result or '.saveAsTable(' in result or '.writeTo(' in result


def test_adds_spark_iceberg_config():
    source = '''from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("job").getOrCreate()
df = spark.read.parquet("data/")
'''
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert "iceberg" in result.lower()


def test_preserves_groupby():
    source = '''df = spark.read.parquet("data/")
agg = df.groupBy("status").count()
agg.show()
'''
    result = transform_pyspark_file(source, table_name="events", namespace="default")
    assert 'groupBy("status")' in result
    assert "agg.show()" in result

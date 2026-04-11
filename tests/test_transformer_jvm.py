from skills.parquet_to_iceberg.transformers.jvm import transform_jvm_file


def test_transforms_java_spark_read():
    source = '''package com.example;
import org.apache.spark.sql.Dataset;

public class Job {
    Dataset<Row> df = spark.read().parquet("data/events/");
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'spark.read().parquet' not in result
    assert 'format("iceberg")' in result
    assert '"default.events"' in result


def test_transforms_java_spark_write():
    source = '''package com.example;
public class Job {
    public void run(Dataset<Row> df) {
        df.write().mode("overwrite").parquet("output/");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result
    assert 'overwritePartitions()' in result


def test_transforms_scala_spark_read():
    source = '''package com.example
import org.apache.spark.sql.{DataFrame, SparkSession}

object Job {
  def load(spark: SparkSession): DataFrame = spark.read.parquet("data/")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="events", namespace="default")
    assert 'spark.read.parquet' not in result
    assert 'format("iceberg")' in result


def test_transforms_scala_spark_write():
    source = '''object Job {
  def save(df: DataFrame): Unit = df.write.mode("overwrite").parquet("out/")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result


def test_transforms_hive_stored_as_parquet():
    source = '''public class Hive {
    public void run() {
        spark.sql("CREATE TABLE events (id BIGINT, status STRING) STORED AS PARQUET");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'STORED AS PARQUET' not in result
    assert 'USING iceberg' in result


def test_transforms_save_as_table():
    source = '''public class Hive {
    public void run(Dataset<Row> df) {
        df.write().mode("overwrite").saveAsTable("events");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'saveAsTable' not in result
    assert 'writeTo("default.events")' in result
    assert 'createOrReplace()' in result


def test_preserves_unrelated_lines():
    source = '''public class Job {
    private static final String APP = "etl";
    public Dataset<Row> load() {
        Dataset<Row> df = spark.read().parquet("in/");
        return df.filter("status = 'active'");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert 'APP = "etl"' in result
    assert "filter(\"status = 'active'\")" in result


def test_java_spark_write_preserves_partition_by():
    source = '''df.write().partitionBy("day").mode("overwrite").parquet("out/");
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert '.parquet(' not in result
    assert 'writeTo("default.events")' in result
    assert 'partitionBy' in result or 'TODO' in result


def test_scala_multi_line_write_chain_save_as_table():
    """Regression: LearningSparkV2 SortMergeJoinBucketed_7_6.scala case."""
    source = '''object Example {
  usersDF.write.format("parquet")
    .bucketBy(8, "uid")
    .mode(SaveMode.OverWrite)
    .saveAsTable("UsersTbl")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="UsersTbl", namespace="default")
    assert '.format("parquet")' not in result, result
    assert 'saveAsTable' not in result, result
    assert 'writeTo("default.UsersTbl")' in result, result
    assert 'createOrReplace()' in result, result
    # bucketBy should be preserved as a TODO
    assert 'bucketBy' in result, result


def test_scala_multi_line_read_chain():
    source = '''object Example {
  val df = spark.read
    .format("parquet")
    .option("header", "true")
    .load("data/events/")
}
'''
    result = transform_jvm_file(source, language="scala", table_name="events", namespace="default")
    assert '.format("parquet")' not in result, result
    assert '.load("data/events/")' not in result, result
    assert 'format("iceberg")' in result, result
    assert '.load("default.events")' in result, result


def test_java_multi_line_write_chain():
    source = '''public class Job {
    public void run(Dataset<Row> df) {
        df.write()
          .format("parquet")
          .mode("overwrite")
          .save("out/");
    }
}
'''
    result = transform_jvm_file(source, language="java", table_name="events", namespace="default")
    assert '.format("parquet")' not in result, result
    assert '.save("out/")' not in result, result
    assert 'writeTo("default.events")' in result, result
    assert 'overwritePartitions()' in result, result

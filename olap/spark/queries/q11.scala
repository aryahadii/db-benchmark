import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")
val partsupps = spark.read.parquet("hdfs://namenode:8020/partsupp.{}")

val mainClause = nations.filter($"n_name" === "EGYPT").join(suppliers, $"n_nationkey" === suppliers("s_nationkey")).join(partsupps, $"s_suppkey" === partsupps("ps_suppkey")).select($"ps_partkey", ($"ps_supplycost" * $"ps_availqty").as("tmp_value"))
val havingClause = mainClause.agg(sum("tmp_value").as("total_value"))

mainClause.groupBy($"ps_partkey").agg(sum("value").as("value")).join(havingClause, $"value" > $"total_value" * 0.0001).sort($"value".desc).show()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val regions = spark.read.parquet("hdfs://namenode:8020/region.{}")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/suppliers.{}")

val region = regions.filter($"r_name" === "AMERICA").join(nations, $"r_regionkey" === nations("n_regionkey")).join(suppliers, $"n_nationkey" === suppliers("s_nationkey")).join(partsupp, suppliers("s_suppkey") === partsupp("ps_supkey"))

val part = parts.filter($"p_size" === 42).filter(($"p_type").endsWith("STEEL")).join(region, region("ps_partkey") === $"p_partkey")

val minCost = part.groupBy(region("ps_partkey")).agg(min("ps_supplycost").as("min"))

part.join(minCost, part("ps_partkey") === minCost("ps_partkey")).filter(part("ps_supplycost") === minCost("min")).select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment").sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey").limit(100).show()

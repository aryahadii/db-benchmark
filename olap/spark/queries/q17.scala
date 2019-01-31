import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")
val partsupps = spark.read.parquet("hdfs://namenode:8020/partsupp.{}")

parts.filter($"p_brand" === "Brand#13").filter($"p_container" === "LG CASE").select($"p_partkey").join(lineitems, $"p_partkey" === lineitems("l_partkey"), "left_outer").groupBy("p_partkey").agg((avg($"l_quantity") * 0.2).as("avg_quantity")).select($"p_partkey".as("key"), $"avg_quantity").join(part, $"key" === part("p_partkey")).filter($"l_quantity" < $"avg_quantity").agg(sum($"l_extendedprice") / 7.0).show()

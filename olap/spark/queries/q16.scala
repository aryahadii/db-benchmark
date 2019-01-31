import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")
val partsupps = spark.read.parquet("hdfs://namenode:8020/partsupp.{}")

val isSizeValid = udf { (x: Int) => List(1, 7, 21, 15, 14, 41, 4, 28).contain(x) }
val typeMatch = udf { (x: String) => x.startsWith("LARGE BRUSHED") }
val comment = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }


val part = parts.filter($"p_brand" !== "Brand#13").filter(!typeMatch($"p_type")).filter(isSizeValid($"p_size")).select($"p_partkey", $"p_brand", $"p_type", $"p_size")

suppliers.filter(!comment($"s_comment")).join(partsupps, $"s_suppkey" === partsupps("ps_suppkey")).select($"ps_partkey", $"ps_suppkey").join(part, $"ps_partkey" === part("p_partkey")).groupBy($"p_brand", $"p_type", $"p_size").agg(countDistinct($"ps_suppkey").as("supplier_cnt")).sort($"supplier_cnt".desc, $"p_brand", $"p_type", $"p_size")

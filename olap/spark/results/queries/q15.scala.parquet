import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.parquet")

val revenue = lineitems.filter($"l_shipdate" >= "1998-10-10").filter($"l_shipdate" < "1999-01-10").select($"l_suppkey", ($"l_extendedprice" * (1 - $"l_discount")).as("with_discount")).groupBy($"l_suppkey").agg(sum($"with_discount").as("total_value"))

revenue.agg(max($"total_value").as("max_total")).join(revenue, $"max_total" === revenue("total_value")).join(suppliers, $"l_suppkey" === suppliers("s_suppkey")).select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total_value").sort($"s_suppkey").show()

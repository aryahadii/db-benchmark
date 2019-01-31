import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val orders = spark.read.parquet("hdfs://namenode:8020/order.{}")
val customers = spark.read.parquet("hdfs://namenode:8020/customer.{}")

customers.join(orders, $"c_custkey" === orders("o_custkey") && !orders("o_comment").matches("%express%accounts%"), "left_outer").groupBy($"o_custkey").agg(count($"o_orderkey").as("c_count")).groupBy($"c_count").agg(count($"o_custkey").as("custdist")).sort($"custdist".desc, $"c_count".desc).show()

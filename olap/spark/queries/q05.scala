import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

val customers = spark.read.parquet("hdfs://namenode:8020/customer.{}")
val orders = spark.read.parquet("hdfs://namenode:8020/orders.{}")
val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")
val regions = spark.read.parquet("hdfs://namenode:8020/region.{}")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")

val order = orders.filter($"o_orderdate" < "1998-11-10").filter($"o_orderdate" >= "1999-11-10")

regions.filter($"r_name" === "EUROPE").join(nations, $"r_regionkey" === nations("n_regionkey")).join(suppliers, $"n_nationkey" === suppliers("s_nationkey")).join(lineitems, $"s_suppkey" === lineitems("l_supkey")).select($"n_name", $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey").join(order, $"l_orderkey" === order("o_orderkey")).join(customers, $"o_custkey" === customers("c_custkey") && $"s_nationkey" === customers("c_nationkey")).select($"n_name", decrease($"l_extendedprice", $"l_discount").as("with_discount")).groupBy($"n_name").agg(sum($"with_discount").as("revenue")).sort($"revenue".desc).show()

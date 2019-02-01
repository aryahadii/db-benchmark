import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/orders.parquet")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.parquet")
val parts = spark.read.parquet("hdfs://namenode:8020/part.parquet")
val partSupps = spark.read.parquet("hdfs://namenode:8020/partsupp.parquet")

val extYear = udf { (x: String) => x.substring(0, 4) }
val expr = udf { (x: Double, y: Double, v: Double, w: Double) => x * (1 - y) - (v * w) }

val linePart = parts.filter($"p_name".contains("papaya")).join(lineitems, $"p_partkey" === lineitems("l_partkey"))
val nationSupplier = nations.join(suppliers, $"n_nationkey" === suppliers("s_nationkey"))

linePart.join(nationSupplier, $"l_suppkey" === nationSupplier("s_suppkey")).join(partSupps, $"l_suppkey" === partSupps("ps_suppkey")).filter($"l_partkey" === partSupps("ps_partkey")).join(orders, $"l_orderkey" === orders("o_orderkey")).select($"n_name", extYear($"o_orderdate").as("o_year"), ($"l_extendedprice" * (1 - $"l_discount") - ($"ps_supplycost" * $"l_quantity").as("amount")).groupBy($"n_name", $"o_year").agg(sum($"amount")).sort($"n_name", $"o_year".desc).show()

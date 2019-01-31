import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val startWithLemon = udf { (x: String) => x.startsWith("lemon") }

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")

val lineitem = lineitems.filter($"l_shipdate" >= "1998-10-10").filter($"l_shipdate" < "1999-10-10").groupBy($"l_partkey", $"l_suppkey").agg((sum($"l_quantity") * 0.5).as("sum_quantity"))
val nation = nations.filter($"n_name" === "EGYPT")
val nationSupplier = suppliers.select($"s_suppkey", $"s_name", $"s_nationkey", $"s_address").join(nation, $"s_nationkey" === nation("n_nationkey"))

parts.filter(startWithLemon($"p_name").select($"p_partkey").distinct.join(partsupps, $"p_partkey" === partsupps("ps_partkey")).join(lineitem, $"ps_suppkey" === lineitem("l_suppkey") && $"ps_partkey" === lineitem("l_partkey")).filter($"ps_availqty" > $"sum_quantity").select($"ps_suppkey").distinct.join(nationSupplier, $"ps_suppkey" === nationSupplier("s_suppkey")).select($"s_name", $"s_address").sort($"s_name").show()

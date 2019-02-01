import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val regions = spark.read.parquet("hdfs://namenode:8020/region.parquet")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/orders.parquet")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.parquet")
val parts = spark.read.parquet("hdfs://namenode:8020/part.parquet")
val customers = spark.read.parquet("hdfs://namenode:8020/customers.parquet")

val extYear = udf { (x: String) => x.substring(0, 4) }
val isEgypt = udf { (x: String, y: Double) => if (x == "EGYPT") y else 0 }

val region = regions.filter($"r_name" === "MIDDLE EAST")
val part = parts.filter($"p_type" === "SMALL PLATED COPPER")
val order = orders.filter($"o_orderdate" <= "1996-12-31" && $"o_orderdate" >= "1995-01-01")
val nationSupplier = nations.join(suppliers, $"n_nationkey" === suppliers("s_nationkey"))
val line = lineitems.select($"l_partkey", $"l_suppkey", $"l_orderkey", ($"l_extendedprice" * (1 - $"l_discount")).as("with_discount")).join(fpart, $"l_partkey" === part("p_partkey")).join(nationSupplier, $"l_suppkey" === nationSupplier("s_suppkey"))

nations.join(regions, $"n_regionkey" === regions("r_regionkey")).select($"n_nationkey").join(customers, $"n_nationkey" === customers("c_nationkey")).select($"c_custkey").join(order, $"c_custkey" === order("o_custkey")).select($"o_orderkey", $"o_orderdate").join(line, $"o_orderkey" === lineitem("l_orderkey")).select(extYear($"o_orderdate").as("o_year"), $"with_discount", isEgypt($"n_name", $"with_discount").as("egypt_with_discount")).groupBy($"o_year").agg(sum($"egypt_with_discount") / sum("with_discount")).sort($"o_year").show()

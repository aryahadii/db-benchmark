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

val customer = customers.filter($"c_mktsegment" === "MACHINERY")
val order = orders.filter($"o_orderdate" < "1998-11-20")
val lineitem = lineitems.filter($"l_shipdate" > "1998-11-20")

customer.join(order, $"c_custkey" === order("o_custkey")).select($"o_orderkey", $"o_orderdate", $"o_shippriority").join(lineitem, $"o_orderkey" === lineitems("l_orderkey")).select($"l_orderkey", decrease($"l_extendedprice", $"l_discount").as("with_discount"), $"o_orderdate", $"o_shippriority").groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"with_discount").as("revenue")).sort($"revenue".desc, $"o_orderdate").limit(10).show()

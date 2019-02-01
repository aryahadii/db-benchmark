import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/orders.parquet")

val lineitem = lineitems.filter($"l_returnflag" === "R")

orders.filter($"o_orderdate" < "1999-02-20").filter($"o_orderdate" >= "1998-11-20").join(customers, $"o_custkey" === customers("c_custkey")).join(nations, $"c_nationkey" === nations("n_nationkey")).join(lineitem, $"o_orderkey" === lineitem("l_orderkey")).select($"c_custkey", $"c_name", ($"l_extendedprice" * (1 - $"l_discount")).as("with_discount"), $"c_acctbal", $"n_name", $"c_address", $"c_phone", $"c_comment").groupBy($"c_custkey", $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment").agg(sum($"with_discount").as("revenue")).sort($"revenue".desc).limit(20).show()

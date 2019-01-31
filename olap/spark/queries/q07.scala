import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")
val orders = spark.read.parquet("hdfs://namenode:8020/orders.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")
val customers = spark.read.parquet("hdfs://namenode:8020/customers.{}")

val extractYear = udf { (x: String) => x.substring(0, 4) }

val nation = nations.filter($"n_name" === "EGYPT" || $"n_name" === "ETHIOPIA")
val lineitem = lineitems.filter($"l_shipdate" >= "1995-01-01").filter($"l_shipdate" <= "1996-12-31")
val supplierNation = nation.join(suppliers, $"n_nationkey" === suppliers("s_nationkey")).join(fline, $"s_suppkey" === lineitem("l_suppkey")).select($"n_name".as("supp_nation"), $"l_orderkey", $"l_extendedprice", $"l_discount", $"l_shipdate")

nation.join(customers, $"n_nationkey" === customers("c_nationkey")).join(orders, $"c_custkey" === orders("o_custkey")).select($"n_name".as("cust_nation"), $"o_orderkey").join(supplierNation, $"o_orderkey" === supplierNation("l_orderkey")).filter($"supp_nation" === "EGYPT" && $"cust_nation" === "ETHIOPIA" || $"supp_nation" === "ETHIOPIA" && $"cust_nation" === "EGYPT").select($"supp_nation", $"cust_nation", extractYear($"l_shipdate").as("l_year"), ($"l_extendedprice" * (1 - $"l_discount")).as("with_discount")).groupBy($"supp_nation", $"cust_nation", $"l_year").agg(sum($"with_discount").as("revenue")).sort($"supp_nation", $"cust_nation", $"l_year").show()

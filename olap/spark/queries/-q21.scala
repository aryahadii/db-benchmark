import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val nations = spark.read.parquet("hdfs://namenode:8020/nation.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")
val suppliers = spark.read.parquet("hdfs://namenode:8020/supplier.{}")


val confinedLineitem = lineitems.filter($"l_receiptdate" > $"l_commitdate")

val l2 = lineitem.groupBy($"l_orderkey").agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
    .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

val l3 = confinedLineitem.groupBy($"l_orderkey")
    .agg(countDistinct($"l_suppkey").as("suppkey_count"), max($"l_suppkey").as("suppkey_max"))
    .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

nation.filter($"n_name" === "EGYPT").join(suppliers, $"n_nationkey" === suppliers("s_nationkey")).join(lineitems, $"s_suppkey" === lineitems("l_suppkey")).join(orders, $"l_orderkey" === orders("o_orderkey")).filter($"o_orderstatus" === "F")
    .join(line1, $"l_orderkey" === line1("key"))
    .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_suppkey" == $"max_suppkey"))
    .select($"s_name", $"l_orderkey", $"l_suppkey")
    .join(line2, $"l_orderkey" === line2("key"), "left_outer")
    .select($"s_name", $"l_orderkey", $"l_suppkey", $"suppkey_count", $"suppkey_max")
    .filter($"suppkey_count" === 1 && $"l_suppkey" === $"suppkey_max")
    .groupBy($"s_name")
    .agg(count($"l_suppkey").as("numwait"))
    .sort($"numwait".desc, $"s_name")
    .limit(100)

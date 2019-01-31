import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")

lineitems.filter($"l_shipdate" <= "1998-09-01").groupBy($"l_returnflag", $"l_linestatus").agg(sum($"l_quantity"), sum($"l_extendedprice"), sum($"l_extendedprice" * (1 - $"l_discount")), sum($"l_extendedprice" * (1 - $"l_discount") * (1 + $"l_tax")), avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity")).sort($"l_returnflag", $"l_linestatus").show()

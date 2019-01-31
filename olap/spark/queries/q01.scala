import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
val increase = udf { (x: Double, y: Double) => x * (1 + y) }

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")

lineitems.filter($"l_shipdate" <= "1998-09-01").groupBy($"l_returnflag", $"l_linestatus").agg(sum($"l_quantity"), sum($"l_extendedprice"), sum(decrease($"l_extendedprice", $"l_discount")), sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")), avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity")).sort($"l_returnflag", $"l_linestatus").show()

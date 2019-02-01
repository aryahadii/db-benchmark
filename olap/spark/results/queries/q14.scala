import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")

parts.join(lineitems, $"l_partkey" === $"p_partkey" && $"l_shipdate" >= "1998-10-05" && $"l_shipdate" < "1995-10-05").select($"p_type", ($"l_extendedprice" * (1 - $"l_discount")).as("with_discount")).agg(sum(if ($"p_type").startsWith("PROMO") $"value" else 0) * 100 / sum($"with_discount")).show()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")

lineitems.filter($"l_shipdate" >= "1998-11-20").filter($"l_shipdate" < "1998-11-20").filter($"l_discount" >= 0.49).filter($"l_discount" <= 0.51 && $"l_quantity" < 2).agg(sum($"l_extendedprice" * $"l_discount")).show()

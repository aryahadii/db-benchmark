import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }


val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")

parts.join(lineitems, $"l_partkey" === $"p_partkey" && $"l_shipdate" >= "1998-10-05" && $"l_shipdate" < "1995-10-05").select($"p_type", decrease($"l_extendedprice", $"l_discount").as("with_discount")).agg(sum(promo($"p_type")) * 100 / sum($"with_discount")).show()

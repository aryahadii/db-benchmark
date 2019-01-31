import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val small = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
val medium = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
val large = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }
val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.{}")
val parts = spark.read.parquet("hdfs://namenode:8020/part.{}")

parts.join(lineitems, $"l_partkey" === $"p_partkey").filter($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG").filter($"l_shipinstruct" === "DELIVER IN PERSON").filter((
    ($"p_brand" === "Brand#13") && small($"p_container") && $"l_quantity" >= 10 && $"l_quantity" <= 20 && $"p_size" >= 1 && $"p_size" <= 5) ||
    (($"p_brand" === "Brand#11") && medium($"p_container") && $"l_quantity" >= 12 && $"l_quantity" <= 22 && $"p_size" >= 1 && $"p_size" <= 10) ||
    (($"p_brand" === "Brand#25") && large("LG CASE", "LG BOX", "LG PACK", "LG PKG") && $"l_quantity" >= 15 && $"l_quantity" <= 25 && $"p_size" >= 1 && $"p_size" <= 15)).select(decrease($"l_extendedprice", $"l_discount").as("volume")).agg(sum("volume")).show()

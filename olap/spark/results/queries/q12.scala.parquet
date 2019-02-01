import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/order.parquet")

val isHigh = udf { (x: String) => if (x == "1-URGENT" || x == "2-HIGH") 1 else 0 }
val isLow = udf { (x: String) => if (x != "1-URGENT" && x != "2-HIGH") 1 else 0 }

lineitems.filter($"l_shipmode" === "TRUCK" || $"l_shipmode" === "MAIL").filter($"l_commitdate" < $"l_receiptdate").filter($"l_shipdate" < $"l_commitdate").filter($"l_receiptdate" >= "1998-10-10").filter($"l_receiptdate" < "1998-10-10").join(orders, $"l_orderkey" === orders("o_orderkey")).select($"l_shipmode", $"o_orderpriority").groupBy($"l_shipmode").agg(sum(isHigh($"o_orderpriority")).as("high_line_count"), sum(isLow($"o_orderpriority")).as("low_line_count")).sort($"l_shipmode").show()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val customers = spark.read.parquet("hdfs://namenode:8020/customer.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/order.parquet")
val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")

val order = orders.filter($"o_orderdate" >= "1998-11-20".filter($"o_orderdate" < "1999-02-20")
val lineitem = lineitem.filter($"l_commitdate" < $"l_receiptdate").select($"l_orderkey").distinct

lineitem.join(order, $"l_orderkey" === order("o_orderkey")).groupBy($"o_orderpriority").agg(count($"o_orderpriority")).sort($"o_orderpriority").show()

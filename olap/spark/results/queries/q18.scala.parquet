import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val lineitems = spark.read.parquet("hdfs://namenode:8020/lineitem.parquet")
val customers = spark.read.parquet("hdfs://namenode:8020/customer.parquet")
val orders = spark.read.parquet("hdfs://namenode:8020/order.parquet")

lineitems.groupBy($"l_orderkey").agg(sum($"l_quantity").as("sum_quantity")).filter($"sum_quantity" > 0).select($"l_orderkey".as("key"), $"sum_quantity").join(orders, orders("o_orderkey") === $"key").join(lineitems, $"o_orderkey" === lineitems("l_orderkey")).join(customers, customers("c_custkey") === $"o_custkey").select($"l_quantity", $"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").groupBy($"c_name", $"c_custkey", $"o_orderkey", $"o_orderdate", $"o_totalprice").agg(sum("l_quantity")).sort($"o_totalprice".desc, $"o_orderdate").limit(100).show()

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions.udf

val subString = udf { (x: String) => x.substring(0, 2) }
val phone = udf { (x: String) => List("13", "40", "22", "29", "21", "18", "17").contain(x) }

val customers = spark.read.parquet("hdfs://namenode:8020/customer.{}")
val orders = spark.read.parquet("hdfs://namenode:8020/order.{}")

val customer = customers.select($"c_acctbal", $"c_custkey", subString($"c_phone").as("cntrycode")).filter(phone($"cntrycode"))
val balance = customer.filter($"c_acctbal" > 0.0).agg(avg($"c_acctbal").as("avg_acctbal"))

orders.groupBy($"o_custkey").agg($"o_custkey").select($"o_custkey").join(customer, $"o_custkey" === customer("c_custkey"), "right_outer").join(balance).filter($"c_acctbal" > $"avg_acctbal").groupBy($"cntrycode").agg(count($"c_acctbal"), sum($"c_acctbal")).sort($"cntrycode").show()

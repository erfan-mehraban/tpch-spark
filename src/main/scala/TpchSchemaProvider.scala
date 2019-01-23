package main.scala

import org.apache.spark.sql.SparkSession

// TPC-H table schemas
case class Customer(
  c_custkey: Long,
  c_name: String,
  c_address: String,
  c_nationkey: Long,
  c_phone: String,
  c_acctbal: Double,
  c_mktsegment: String,
  c_comment: String)

case class Lineitem(
  l_orderkey: Long,
  l_partkey: Long,
  l_supkey: Long,
  l_linenumber: Long,
  l_quantity: Double,
  l_extendedprice: Double,
  l_discount: Double,
  l_tax: Double,
  l_returnflag: String,
  l_linestatus: String,
  l_shipdate: String,
  l_commitdate: String,
  l_receiptdate: String,
  l_shipinstruct: String,
  l_shipmode: String,
  l_comment: String)

case class Nation(
  n_nationkey: Long,
  n_name: String,
  n_regionkey: Long,
  n_comment: String)

case class Order(
  o_orderkey: Long,
  o_custkey: Long,
  o_orderstatus: String,
  o_totalprice: Double,
  o_orderdate: String,
  o_orderpriority: String,
  o_clerk: String,
  o_shippriority: Long,
  o_comment: String)

case class Part(
  p_partkey: Long,
  p_name: String,
  p_mfgr: String,
  p_brand: String,
  p_type: String,
  p_size: Long,
  p_container: String,
  p_retailprice: Double,
  p_comment: String)

case class Partsupp(
  ps_partkey: Long,
  ps_supkey: Long,
  ps_availqty: Long,
  ps_supplycost: Double,
  ps_comment: String)

case class Region(
  r_regionkey: Long,
  r_name: String,
  r_comment: String)

case class Supplier(
  s_suppkey: Long,
  s_name: String,
  s_address: String,
  s_nationkey: Long,
  s_phone: String,
  s_acctbal: Double,
  s_comment: String)

class TpchSchemaProvider(spark: SparkSession, inputDir: String) {

  val format_type = "com.databricks.spark.avro"

  val dfMap = Map(
    "customer" -> spark.read.format(format_type).load(inputDir+"customer.avro"),
    "lineitem" -> spark.read.format(format_type).load(inputDir+"lineitem.avro"),
    "nation" -> spark.read.format(format_type).load(inputDir+"nation.avro"),
    "region" -> spark.read.format(format_type).load(inputDir+"region.avro"),
    "order" -> spark.read.format(format_type).load(inputDir+"orders.avro"),
    "part" -> spark.read.format(format_type).load(inputDir+"part.avro"),
    "partsupp" -> spark.read.format(format_type).load(inputDir+"partsupp.avro"),
    "supplier" -> spark.read.format(format_type).load(inputDir+"supplier.avro")
  )
  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}

package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 5
 */
class Q05 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val forders = order
      .filter($"o_orderdate" < "1996-10-01"
        && $"o_orderdate" >= "1995-10-01")

    region.filter($"r_name" === "MIDDLE EAST")
      .join(nation,
        $"r_regionkey" === nation("n_regionkey"))
      .join(supplier,
        $"n_nationkey" === supplier("s_nationkey"))
      .join(lineitem,
        $"s_suppkey" === lineitem("l_suppkey"))
      .select($"n_name",
        $"l_extendedprice", $"l_discount", $"l_orderkey", $"s_nationkey")
      .join(forders,
        $"l_orderkey" === forders("o_orderkey"))
      .join(customer,
        $"o_custkey" === customer("c_custkey") && $"s_nationkey" === customer("c_nationkey"))
      .select(
        $"n_name",
        decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"n_name")
      .agg(sum($"value").as("revenue"))
      .sort($"revenue".desc)
  }

}

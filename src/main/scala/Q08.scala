package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 8
 */
class Q08 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val getYear  = udf { (x: String) => x.substring(0, 4) }
    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val isBrazil = udf { (x: String, y: Double) => if (x == "FRANCE") y else 0 }

    val fregion = region.filter($"r_name" === "EUROPE")
    val forder = order
      .filter(
        $"o_orderdate" <= "1996-12-31"
        && $"o_orderdate" >= "1995-01-01")
    val fpart = part.filter($"p_type" === "LARGE BRUSHED STEEL")

    val nat = nation.join(supplier, $"n_nationkey" === supplier("s_nationkey"))

    val line = lineitem
      .select($"l_partkey", $"l_supkey", $"l_orderkey",
      decrease($"l_extendedprice", $"l_discount").as("volume"))
      .join(fpart, $"l_partkey" === fpart("p_partkey"))
      .join(nat, $"l_supkey" === nat("s_suppkey"))

    nation
      .join(fregion, $"n_regionkey" === fregion("r_regionkey"))
      .select($"n_nationkey")
      .join(customer, $"n_nationkey" === customer("c_nationkey"))
      .select($"c_custkey")
      .join(forder, $"c_custkey" === forder("o_custkey"))
      .select($"o_orderkey", $"o_orderdate")
      .join(line, $"o_orderkey" === line("l_orderkey"))
      .select(
        getYear($"o_orderdate").as("o_year"),
        $"volume",
        isBrazil($"n_name", $"volume").as("case_volume"))
      .groupBy($"o_year")
      .agg(sum($"case_volume") / sum("volume"))
      .sort($"o_year")
  }

}

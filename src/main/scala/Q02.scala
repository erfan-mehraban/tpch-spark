package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.min

/**
 * TPC-H Query 2
 */
class Q02 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val continent = region
      .filter($"r_name" === "ASIA")
      .join(nation, $"r_regionkey" === nation("n_regionkey"))
      .join(supplier, $"n_nationkey" === supplier("s_nationkey"))
      .join(partsupp, supplier("s_suppkey") === partsupp("ps_suppkey"))
    //.select($"ps_partkey", $"ps_supplycost", $"s_acctbal", $"s_name", $"n_name", $"s_address", $"s_phone", $"s_comment")

    val part_type = part
      .filter(
        part("p_size") === 30
        && part("p_type").endsWith("TIN")
      ).join(continent,
        continent("ps_partkey") === $"p_partkey")
    //.cache

    val minCost = part_type
      .groupBy(part_type("ps_partkey"))
      .agg(
        min("ps_supplycost").as("min")
      )

    part_type
      .join(minCost,
        part_type("ps_partkey") === minCost("ps_partkey"))
      .filter(part_type("ps_supplycost") === minCost("min"))
      .select("s_acctbal", "s_name", "n_name", "p_partkey", "p_mfgr", "s_address", "s_phone", "s_comment")
      .sort($"s_acctbal".desc, $"n_name", $"s_name", $"p_partkey")
      .limit(100)
  }

}

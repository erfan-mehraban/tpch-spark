package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 16
 */
class Q16 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
    val complains = udf { (x: String) => x.matches(".*Customer.*Complaints.*") }
    val polished = udf { (x: String) => x.startsWith("ECONOMY PLATED") }
    val numbers = udf { (x: Int) => x.toString().matches("10|12|39|28|19|6|35|44") }

    val fparts = part.filter(($"p_brand" !== "Brand#23") && !polished($"p_type") &&
      numbers($"p_size"))
      .select($"p_partkey", $"p_brand", $"p_type", $"p_size")

    supplier.filter(!complains($"s_comment"))
      // .select($"s_suppkey")
      .join(partsupp, $"s_suppkey" === partsupp("ps_suppkey"))
      .select($"ps_partkey", $"ps_suppkey")
      .join(fparts, $"ps_partkey" === fparts("p_partkey"))
      .groupBy($"p_brand", $"p_type", $"p_size")
      .agg(countDistinct($"ps_suppkey").as("supplier_count"))
      .sort($"supplier_count".desc, $"p_brand", $"p_type", $"p_size")
  }

}

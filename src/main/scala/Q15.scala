package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 15

 */
class Q15 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    val revenue = lineitem
      .filter(
        ($"l_shipdate"/1000).cast("timestamp") >= "1993-08-01"
        && ($"l_shipdate"/1000).cast("timestamp") < "1993-11-01")
      .select($"l_supkey", decrease($"l_extendedprice", $"l_discount").as("value"))
      .groupBy($"l_supkey")
      .agg(sum($"value").as("total"))
    // .cache

    revenue.agg(max($"total").as("max_total"))
      .join(revenue, $"max_total" === revenue("total"))
      .join(supplier, $"l_supkey" === supplier("s_suppkey"))
      .select($"s_suppkey", $"s_name", $"s_address", $"s_phone", $"total")
      .sort($"s_suppkey")
  }

}

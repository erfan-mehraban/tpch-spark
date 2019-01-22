package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 13
 */
class Q13 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val special = udf { (x: String) => x.matches(".*unusual.*packages.*") }

    customer
      .join(order,
        $"c_custkey" === order("o_custkey")
        && !special(order("o_comment"))
        , "left_outer")
      .groupBy($"o_custkey")
      .agg(count($"o_orderkey").as("c_count"))
      .groupBy($"c_count")
      .agg(count($"o_custkey").as("custdist"))
      .sort($"custdist".desc, $"c_count".desc)
  }

}

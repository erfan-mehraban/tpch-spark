package main.scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 6
 */
class Q06 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    lineitem
      .filter(
        $"l_shipdate" >= "1994-05-18"
        && $"l_shipdate" < "1995-05-18"
        && $"l_discount" >= (0.3 - 0.01)
        && $"l_discount" <= (0.3 + 0.01)
        && $"l_quantity" < 24)
      .agg(
        sum($"l_extendedprice" * $"l_discount"))
  }

}

package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 14

 */
class Q14 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val reduce = udf { (x: Double, y: Double) => x * (1 - y) }
    val promo = udf { (x: String, y: Double) => if (x.startsWith("PROMO")) y else 0 }

    part
      .join(lineitem,
        $"l_partkey" === $"p_partkey"
        && ($"l_shipdate"/1000).cast("timestamp") >= "1996-03-20"
        && ($"l_shipdate"/1000).cast("timestamp") < "1996-04-20")
      .select($"p_type", reduce($"l_extendedprice", $"l_discount").as("value"))
      .agg(sum
        (promo($"p_type", $"value")) * 100 / sum($"value"))
  }

}

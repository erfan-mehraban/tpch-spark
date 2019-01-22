package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 19
 */
class Q19 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }

    // project part and lineitem first?
    part.join(lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#24") &&
          sm($"p_container") &&
          $"l_quantity" >= 9 && $"l_quantity" <= 19 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#34") &&
            md($"p_container") &&
            $"l_quantity" >= 15 && $"l_quantity" <= 25 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#15") &&
              lg($"p_container") &&
              $"l_quantity" >= 21 && $"l_quantity" <= 31 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
  }

}

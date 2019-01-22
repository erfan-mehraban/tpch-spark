package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 21
 */
class Q21 extends TpchQuery {

  override def execute(spark: SparkSession, schemaProvider: TpchSchemaProvider): DataFrame = {

    import spark.implicits._
    import schemaProvider._

    val fsupplier = supplier.select($"s_suppkey", $"s_nationkey", $"s_name")

    val plineitem = lineitem.select($"l_supkey", $"l_orderkey", $"l_receiptdate", $"l_commitdate")
    //cache

    val flineitem = plineitem.filter($"l_receiptdate" > $"l_commitdate")
    // cache

    val line1 = plineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_supkey").as("suppkey_count"), max($"l_supkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val line2 = flineitem.groupBy($"l_orderkey")
      .agg(countDistinct($"l_supkey").as("suppkey_count"), max($"l_supkey").as("suppkey_max"))
      .select($"l_orderkey".as("key"), $"suppkey_count", $"suppkey_max")

    val forder = order.select($"o_orderkey", $"o_orderstatus")
      .filter($"o_orderstatus" === "F")

    nation.filter($"n_name" === "IRAN")
      .join(fsupplier, $"n_nationkey" === fsupplier("s_nationkey"))
      .join(flineitem, $"s_suppkey" === flineitem("l_supkey"))
      .join(forder, $"l_orderkey" === forder("o_orderkey"))
      .join(line1, $"l_orderkey" === line1("key"))
      .filter($"suppkey_count" > 1 || ($"suppkey_count" == 1 && $"l_supkey" == $"max_suppkey"))
      .select($"s_name", $"l_orderkey", $"l_supkey")
      .join(line2, $"l_orderkey" === line2("key"), "left_outer")
      .select($"s_name", $"l_orderkey", $"l_supkey", $"suppkey_count", $"suppkey_max")
      .filter($"suppkey_count" === 1 && $"l_supkey" === $"suppkey_max")
      .groupBy($"s_name")
      .agg(count($"l_supkey").as("numwait"))
      .sort($"numwait".desc, $"s_name")
      .limit(100)
  }

}

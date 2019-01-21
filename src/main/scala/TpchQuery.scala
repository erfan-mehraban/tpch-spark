package main.scala

import scala.sys.process.Process
import org.apache.spark.sql.SparkSession
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(spark: SparkSession, tpchSchemaProvider: TpchSchemaProvider): DataFrame
}

object TpchQuery {

  def executeQueries(spark: SparkSession, schemaProvider: TpchSchemaProvider, queryNum: Int): ListBuffer[(String, Float)] = {

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != -1) {
      fromNum = queryNum;
      toNum = queryNum;
    }

    for (queryNo <- fromNum to toNum) {
      val start_time = System.nanoTime()
      val query = Class.forName(f"main.scala.Q${queryNo}%02d").newInstance.asInstanceOf[TpchQuery]
      val dstat_process = Process(s"dstat --full --output /root/"+queryNo+".dstat").run
      query.execute(spark, schemaProvider).collect()
      dstat_process.destroy()
      val end_time = System.nanoTime()
      val elapsed = (end_time - start_time) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)
    }

    return results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = -1;
    if (args.length > 0)
      queryNum = args(0).toInt
    var extension = args(1)

    val spark = SparkSession
      .builder()
      .config("spark.sql.orc.impl", "native")
      .appName("Spark-TPCH Benchmark")
      .getOrCreate()
    
    // read from hdfs
    val INPUT_DIR = "hdfs://namenode:8020/"

    val schemaProvider = new TpchSchemaProvider(spark, INPUT_DIR, extension)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(spark, schemaProvider, queryNum)

    val outFile = new File("TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}

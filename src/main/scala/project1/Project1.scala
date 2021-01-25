package com.gridu.task1

import org.apache.spark.sql.{Dataset, Row, SaveMode, Column}

object project1 {
  
  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types._
  
   val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Project1")
      .master("local")
      .getOrCreate() 
  
      val csvOpts = Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true", "treatEmptyValuesAsNulls" -> "true", "nullValue" -> null)
 
      
val data1 = "src/main/resources/data1.csv"
val data2 = "src/main/resources/data2.csv"

val csv1 = spark.read.options(csvOpts).csv(data1)
val csv2 = spark.read.options(csvOpts).csv(data2)


val data_join = csv1.as("a")
  .join(csv2.as("b"), $"a.positionId" === $"b.positionId")
  .select($"a.positionId", $"a.warehouse", $"a.product", $"b.amount")
  

val agg_results = data_join.select(
   $"warehouse",
   $"product",
   $"amount")
   .groupBy($"warehouse",$"product")
   .agg(max($"amount").as("max"),
       min($"amount").as("min"),
       avg($"amount").as("avg"))
}
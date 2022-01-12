package com.tools

import com.data.RecoveryData.ret_recovered_amt
import org.apache.spark.sql.SparkSession

object Router {
  var dbCon: SparkSession = null

  // Pull Latest Recovery Data For US
  def recovery_data_route(con: SparkSession): Unit = {
    dbCon = con
    println("US Recovery Data")
    println("Patients Recovered: " + ret_recovered_amt().asInstanceOf[Double].toInt)
  }

  // Pull Mortality Rate Data For US
  def mortality_data_route(): Unit = {
    println("US Mortality Rate Data")
  }

  // Pull Infection Rate Data For US
  def infection_data_route(): Unit = {
    println("US Infection Rate Data")
  }
}

package com.tools

import com.data.RecoveryData.recovery_options_menu
import com.data.InfectionData.infections_menu
import org.apache.spark.sql.SparkSession

object Router {
  var dbCon: SparkSession = null

  // Pull Latest Recovery Data For US
  def recovery_data_route(): Unit = {
    println("US Recovery Data")
    recovery_options_menu()
  }

  // Pull Mortality Rate Data For US
  def mortality_data_route(): Unit = {
    println("US Mortality Rate Data")
  }

  // Pull Infection Rate Data For US
  def infection_data_route(): Unit = {
    println("US Infection Rate Data")
    infections_menu()
  }
}

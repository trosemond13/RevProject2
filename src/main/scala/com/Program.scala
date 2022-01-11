package com

import com.data.RecoveredData._

object Program {
  def main(args: Array[String]): Unit = {
    initializer()
    val rec = ret_recovered_amt().asInstanceOf[Double]
    closer()

    println("US Recovered Patients: " + rec.toInt)
  }
}

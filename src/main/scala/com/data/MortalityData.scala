package com.data

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext, SparkSession, SQLImplicits}
import org.apache.spark.{SparkConf, SparkContext}
import scala.io.StdIn
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions.{array, array_position, collect_set, explode, explode_outer, struct, to_json, typedLit}

object MortalityData {
  /*def main(args: Array[String]): Unit = {
    val new_session = new DeathTable()
    new_session.start_session()
    new_session.establishTable()
    new_session.showTables()
    new_session.deathPercentageState()
    new_session.deathMonthlyAvg()
    new_session.springOrSummerTravel()
    new_session.displayTable()
    new_session.closeDeathTable()
    // create a spark session
    // for Windows
    //System.setProperty("hadoop.home.dir", "C:\\winutils")
  }*/

  class DeathTable{
    private var spark: SparkSession = null

    def start_session(): Unit ={
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      spark = SparkSession
        .builder
        .appName("DeathTable")
        .config("spark.master", "local")
        .enableHiveSupport()
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      //import spark.sqlContext.implicits._
    }

    private def createDeathTable(): DataFrame={
      var covidDeaths = spark.read.csv("KaggleData(Complete)/KaggleData(Complete)/time_series_covid_19_deaths_US_complete(Kaggle).csv")
      var headerDeaths = covidDeaths.first()

      covidDeaths = covidDeaths.withColumnRenamed("_c0", "UID")
      covidDeaths = covidDeaths.withColumnRenamed("_c1", "iso2")
      covidDeaths = covidDeaths.withColumnRenamed("_c2", "iso3")
      covidDeaths = covidDeaths.withColumnRenamed("_c3", "code3")
      covidDeaths = covidDeaths.withColumnRenamed("_c4", "FIPS")
      covidDeaths = covidDeaths.withColumnRenamed("_c5", "Admin2")
      covidDeaths = covidDeaths.withColumnRenamed("_c6", "Province_State")
      covidDeaths = covidDeaths.withColumnRenamed("_c7", "Country_Region")
      covidDeaths = covidDeaths.withColumnRenamed("_c8", "Lat")
      covidDeaths = covidDeaths.withColumnRenamed("_c9", "Long_")
      covidDeaths = covidDeaths.withColumnRenamed("_c10", "Combined_Key")
      covidDeaths = covidDeaths.withColumnRenamed("_c11", "Population")
      covidDeaths = covidDeaths.withColumnRenamed("_c21", "January_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c50", "February_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c81", "March_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c111", "April_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c142", "May_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c172", "June_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c203", "July_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c234", "August_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c264", "September_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c295", "October_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c325", "November_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c356", "December_2020")
      covidDeaths = covidDeaths.withColumnRenamed("_c387", "January_2021")
      covidDeaths = covidDeaths.withColumnRenamed("_c415", "February_2021")
      covidDeaths = covidDeaths.withColumnRenamed("_c446", "March_2021")
      covidDeaths = covidDeaths.withColumnRenamed("_c476", "April_2021")
      covidDeaths = covidDeaths.withColumnRenamed("_c478", "May_2021")

      covidDeaths = covidDeaths.filter(row => row != headerDeaths)


      covidDeaths = covidDeaths.withColumn("Late_January_2020", array("_c12", "_c13", "_c14","_c15","_c16", "_c17", "_c18", "_c19","_c20", "January_2020")).drop("_c12", "_c13", "_c14","_c15","_c16", "_c17", "_c18", "_c19","_c20")
      covidDeaths = covidDeaths.withColumn("Early_February_2020", array("_c22", "_c23", "_c24","_c25","_c26", "_c27", "_c28", "_c29","_c30", "_c31")).drop("_c22", "_c23", "_c24","_c25","_c26", "_c27", "_c28", "_c29","_c30", "_c31")
      covidDeaths = covidDeaths.withColumn("Mid_February_2020", array("_c32", "_c33", "_c34","_c35","_c36", "_c37", "_c38", "_c39","_c40", "_c41")).drop("_c32", "_c33", "_c34","_c35","_c36", "_c37", "_c38", "_c39","_c40", "_c41")
      covidDeaths = covidDeaths.withColumn("Late_February_2020", array("_c42", "_c43", "_c44","_c45","_c46", "_c47", "_c48", "_c49","February_2020")).drop("_c42", "_c43", "_c44","_c45","_c46", "_c47", "_c48", "_c49")
      covidDeaths = covidDeaths.withColumn("Early_March_2020", array("_c51", "_c52","_c53","_c54", "_c55", "_c56", "_c57","_c58", "_c59","_c60" )).drop("_c51", "_c52","_c53","_c54", "_c55", "_c56", "_c57","_c58", "_c59","_c60")
      covidDeaths = covidDeaths.withColumn("Mid_March_2020", array("_c61", "_c62","_c63","_c64", "_c65", "_c66", "_c67","_c68", "_c69","_c70")).drop("_c61", "_c62","_c63","_c64", "_c65", "_c66", "_c67","_c68", "_c69","_c70")
      covidDeaths = covidDeaths.withColumn("Late_March_2020", array("_c71", "_c72","_c73","_c74", "_c75", "_c76", "_c77","_c78", "_c79","_c80", "March_2020")).drop("_c71", "_c72","_c73","_c74", "_c75", "_c76", "_c77","_c78", "_c79","_c80")
      covidDeaths = covidDeaths.withColumn("Early_April_2020", array("_c82", "_c83", "_c84","_c85","_c86", "_c87", "_c88", "_c89","_c90","_c91")).drop("_c82", "_c83", "_c84","_c85","_c86", "_c87", "_c88", "_c89","_c90","_c91")
      covidDeaths = covidDeaths.withColumn("Mid_April_2020", array("_c92", "_c93", "_c94","_c95","_c96", "_c97", "_c98", "_c99","_c100", "_c101")).drop("_c92", "_c93", "_c94","_c95","_c96", "_c97", "_c98", "_c99","_c100", "_c101")
      covidDeaths = covidDeaths.withColumn("Late_April_2020", array("_c102", "_c103", "_c104","_c105","_c106", "_c107", "_c108", "_c109","_c110", "April_2020")).drop("_c102", "_c103", "_c104","_c105","_c106", "_c107", "_c108", "_c109","_c110")
      covidDeaths = covidDeaths.withColumn("Early_May_2020", array("_c112", "_c113", "_c114","_c115","_c116", "_c117", "_c118", "_c119","_c120", "_c121")).drop("_c112", "_c113", "_c114","_c115","_c116", "_c117", "_c118", "_c119","_c120", "_c121")
      covidDeaths = covidDeaths.withColumn("Mid_May_2020", array("_c122", "_c123", "_c124","_c125","_c126", "_c127", "_c128", "_c129","_c130", "_c131")).drop("_c122", "_c123", "_c124","_c125","_c126", "_c127", "_c128", "_c129","_c130", "_c131")
      covidDeaths = covidDeaths.withColumn("Late_May_2020", array("_c132", "_c133", "_c134","_c135","_c136", "_c137", "_c138", "_c139","_c140", "_c141","May_2020")).drop("_c132", "_c133", "_c134","_c135","_c136", "_c137", "_c138", "_c139","_c140", "_c141")
      covidDeaths = covidDeaths.withColumn("Early_June_2020", array("_c143", "_c144", "_c145","_c146","_c147", "_c148", "_c149", "_c150","_c151","_c152")).drop("_c143", "_c144", "_c145","_c146","_c147", "_c148", "_c149", "_c150","_c151","_c152")
      covidDeaths = covidDeaths.withColumn("Mid_June_2020", array("_c153", "_c154", "_c155","_c156","_c157", "_c158", "_c159", "_c160","_c161","_c162")).drop("_c153", "_c154", "_c155","_c156","_c157", "_c158", "_c159", "_c160","_c161","_c162")
      covidDeaths = covidDeaths.withColumn("Late_June_2020", array("_c163", "_c164", "_c165","_c166","_c167", "_c168", "_c169", "_c170","_c171", "June_2020")).drop("_c163", "_c164", "_c165","_c166","_c167", "_c168", "_c169", "_c170","_c171")
      covidDeaths = covidDeaths.withColumn("Early_July_2020", array("_c173", "_c174", "_c175","_c176","_c177", "_c178", "_c179", "_c180","_c181", "_c182")).drop("_c173", "_c174", "_c175","_c176","_c177", "_c178", "_c179", "_c180","_c181", "_c182")
      covidDeaths = covidDeaths.withColumn("Mid_July_2020", array("_c183", "_c184", "_c185","_c186","_c187", "_c188", "_c189", "_c190","_c191", "_c192")).drop("_c183", "_c184", "_c185","_c186","_c187", "_c188", "_c189", "_c190","_c191", "_c192")
      covidDeaths = covidDeaths.withColumn("Late_July_2020", array("_c193", "_c194", "_c195","_c196","_c197", "_c198", "_c199", "_c200","_c201", "_c202", "July_2020")).drop("_c193", "_c194", "_c195","_c196","_c197", "_c198", "_c199", "_c200","_c201", "_c202")
      covidDeaths = covidDeaths.withColumn("Early_August_2020", array("_c204", "_c205", "_c206","_c207","_c208", "_c209", "_c210", "_c211","_c212", "_c213")).drop("_c204", "_c205", "_c206","_c207","_c208", "_c209", "_c210", "_c211","_c212", "_c213")
      covidDeaths = covidDeaths.withColumn("Mid_August_2020", array("_c214", "_c215", "_c216","_c217","_c218", "_c219", "_c220", "_c221","_c222", "_c223")).drop("_c214", "_c215", "_c216","_c217","_c218", "_c219", "_c220", "_c221","_c222", "_c223")
      covidDeaths = covidDeaths.withColumn("Late_August_2020", array("_c224", "_c225", "_c226","_c227","_c228", "_c229", "_c230", "_c231","_c232", "_c233","August_2020")).drop("_c224", "_c225", "_c226","_c227","_c228", "_c229", "_c230", "_c231","_c232", "_c233")
      covidDeaths = covidDeaths.withColumn("Early_September_2020", array("_c235", "_c236", "_c237","_c238","_c239", "_c240", "_c241", "_c242","_c243","_c244")).drop("_c235", "_c236", "_c237","_c238","_c239", "_c240", "_c241", "_c242","_c243","_c244")
      covidDeaths = covidDeaths.withColumn("Mid_September_2020", array("_c245", "_c246", "_c247","_c248","_c249", "_c250", "_c251", "_c252","_c253","_c254")).drop("_c245", "_c246", "_c247","_c248","_c249", "_c250", "_c251", "_c252","_c253","_c254")
      covidDeaths = covidDeaths.withColumn("Late_September_2020", array("_c255", "_c256", "_c257","_c258","_c259", "_c260", "_c261", "_c262","_c263", "September_2020")).drop("_c255", "_c256", "_c257","_c258","_c259", "_c260", "_c261", "_c262","_c263")
      covidDeaths = covidDeaths.withColumn("Early_October_2020", array("_c265", "_c266", "_c267","_c268","_c269", "_c270", "_c271", "_c272","_c273", "_c274")).drop("_c265", "_c266", "_c267","_c268","_c269", "_c270", "_c271", "_c272","_c273", "_c274")
      covidDeaths = covidDeaths.withColumn("Mid_October_2020", array("_c275", "_c276", "_c277","_c278","_c279", "_c280", "_c281", "_c282","_c283", "_c284")).drop("_c275", "_c276", "_c277","_c278","_c279", "_c280", "_c281", "_c282","_c283", "_c284")
      covidDeaths = covidDeaths.withColumn("Late_October_2020", array("_c285", "_c286", "_c287","_c288","_c289", "_c290", "_c291", "_c292","_c293", "_c294","October_2020")).drop("_c285", "_c286", "_c287","_c288","_c289", "_c290", "_c291", "_c292","_c293", "_c294")
      covidDeaths = covidDeaths.withColumn("Early_November_2020", array("_c296", "_c297", "_c298","_c299","_c300", "_c301", "_c302", "_c303","_c304","_c305")).drop("_c296", "_c297", "_c298","_c299","_c300", "_c301", "_c302", "_c303","_c304","_c305")
      covidDeaths = covidDeaths.withColumn("Mid_November_2020", array("_c306", "_c307", "_c308","_c309","_c310", "_c311", "_c312", "_c313","_c314","_c315")).drop("_c306", "_c307", "_c308","_c309","_c310", "_c311", "_c312", "_c313","_c314","_c315")
      covidDeaths = covidDeaths.withColumn("Late_November_2020", array("_c316", "_c317", "_c318","_c319","_c320", "_c321", "_c322", "_c323","_c324","November_2020")).drop("_c316", "_c317", "_c318","_c319","_c320", "_c321", "_c322", "_c323","_c324")
      covidDeaths = covidDeaths.withColumn("Early_December_2020", array("_c326", "_c327", "_c328","_c329","_c330", "_c331", "_c332", "_c333","_c334","_c335")).drop("_c326", "_c327", "_c328","_c329","_c330", "_c331", "_c332", "_c333","_c334","_c335")
      covidDeaths = covidDeaths.withColumn("Mid_December_2020", array("_c336", "_c337", "_c338","_c339","_c340", "_c341", "_c342", "_c343","_c344","_c345")).drop("_c336", "_c337", "_c338","_c339","_c340", "_c341", "_c342", "_c343","_c344","_c345")
      covidDeaths = covidDeaths.withColumn("Late_December_2020", array("_c346", "_c347", "_c348","_c349","_c350", "_c351", "_c352", "_c353","_c354","_c355","December_2020")).drop("_c346", "_c347", "_c348","_c349","_c350", "_c351", "_c352", "_c353","_c354","_c355")
      covidDeaths = covidDeaths.withColumn("Early_January_2021", array("_c357", "_c358", "_c359","_c360","_c361", "_c362", "_c363", "_c364","_c365","_c366")).drop("_c357","_c358", "_c359","_c360","_c361", "_c362", "_c363", "_c364","_c365","_c366")
      covidDeaths = covidDeaths.withColumn("Mid_January_2021", array("_c367", "_c368", "_c369","_c370","_c371", "_c372", "_c373", "_c374","_c375","_c376")).drop("_c367", "_c368", "_c369","_c370","_c371", "_c372", "_c373", "_c374","_c375","_c376")
      covidDeaths = covidDeaths.withColumn("Late_January_2021", array("_c377", "_c378", "_c379","_c380","_c381", "_c382", "_c383", "_c384","_c385","_c386","January_2021")).drop("_c377", "_c378", "_c379","_c380","_c381", "_c382", "_c383", "_c384","_c385","_c386")
      covidDeaths = covidDeaths.withColumn("Early_February_2021", array("_c388", "_c389", "_c390","_c391","_c392", "_c393", "_c394", "_c395","_c396","_c397")).drop("_c388", "_c389", "_c390","_c391","_c392", "_c393", "_c394", "_c395","_c396","_c397")
      covidDeaths = covidDeaths.withColumn("Mid_February_2021", array("_c398", "_c399", "_c400","_c401","_c402", "_c403", "_c404", "_c405","_c406","_c407")).drop("_c398", "_c399", "_c400","_c401","_c402", "_c403", "_c404", "_c405","_c406","_c407")
      covidDeaths = covidDeaths.withColumn("Late_February_2021", array("_c408", "_c409", "_c410","_c411","_c412", "_c413", "_c414", "February_2021")).drop("_c408", "_c409", "_c410","_c411","_c412", "_c413", "_c414")
      covidDeaths = covidDeaths.withColumn("Early_March_2021", array("_c416", "_c417", "_c418","_c419","_c420", "_c421", "_c422", "_c423","_c424","_c425")).drop("_c416", "_c417", "_c418","_c419","_c420", "_c421", "_c422", "_c423","_c424","_c425")
      covidDeaths = covidDeaths.withColumn("Mid_March_2021", array("_c426", "_c427", "_c428","_c429","_c430", "_c431", "_c432", "_c433","_c434","_c435")).drop("_c426", "_c427", "_c428","_c429","_c430", "_c431", "_c432", "_c433","_c434","_c435")
      covidDeaths = covidDeaths.withColumn("Late_March_2021", array("_c436", "_c437", "_c438","_c439","_c440", "_c441", "_c442", "_c443","_c444","_c445","March_2021")).drop("_c436", "_c437", "_c438","_c439","_c440", "_c441", "_c442", "_c443","_c444","_c445")
      covidDeaths = covidDeaths.withColumn("Early_April_2021", array("_c447", "_c448", "_c449","_c450","_c451", "_c452", "_c453", "_c454","_c455","_c456")).drop("_c447", "_c448", "_c449","_c450","_c451", "_c452", "_c453", "_c454","_c455","_c456")
      covidDeaths = covidDeaths.withColumn("Mid_April_2021", array("_c457", "_c458", "_c459","_c460","_c461", "_c462", "_c463", "_c464","_c465","_c466")).drop("_c457", "_c458", "_c459","_c460","_c461", "_c462", "_c463", "_c464","_c465","_c466")
      covidDeaths = covidDeaths.withColumn("Late_April_2021", array("_c467", "_c468", "_c469","_c470","_c471", "_c472", "_c473", "_c474","_c475","April_2021")).drop("_c467", "_c468", "_c469","_c470","_c471", "_c472", "_c473", "_c474","_c475")
      covidDeaths = covidDeaths.withColumn("Early_May_2021", array("_c477", "May_2021")).drop("_c477")

      return covidDeaths
    }

    //createDeathTable().show(false)
    //val sampleGetSomething = covidDeaths.withColumn("Result", array_position("Early_May_2021","107"))
    def establishTable(): Unit={
      createDeathTable().createOrReplaceTempView("CovidDeathsUS")
    }
    def showTables(): Unit = {
      spark.sql("show tables").show
    }
    def displayTable(): Unit ={
      spark.sql("Select * From CovidDeathsUs").show(false)
    }

    def deathPercentageState(): Unit ={
      var sum = 0
      var totalUSDeath = 0

      print("Enter month to retrieve possible death information involving covid-19: \n>")
      var monthCovid = StdIn.readLine()//TODO I/O

      print("Enter year to retrieve possible death information involving covid-19: \n>")
      var yearCovid = StdIn.readLine()//TODO I/O

      print("Enter state name to retrieve possible death information involving covid-19: \n>")
      var stateName = StdIn.readLine()//TODO I/O

      var temp2 = spark.sql(s"Select ${monthCovid}_${yearCovid} From CovidDeathsUs Where Combined_Key like '%, $stateName, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")

      val temp2Num = temp2.count().toInt
      val t1 = temp2.take(temp2Num)

      for(i<-0 to temp2Num-1){
        sum = sum + t1(i).getString(0).toInt


      }

      var restTemp = spark.sql(s"Select ${monthCovid}_${yearCovid} From CovidDeathsUs Where Admin2 != 'null' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      val restTempNum = restTemp.count.toInt
      val r1 = restTemp.take(restTempNum)
      for(i<-0 to restTempNum-1){
        totalUSDeath = totalUSDeath + r1(i).getString(0).toInt
      }

      val percentageState  = ((sum.toFloat/totalUSDeath).toDouble)
      val percentageStateVal = percentageState * 100

      println("The percentage is: ")
      println(f"$percentageStateVal%.2f" + "%")

      if(percentageState < 0.05){
        println("This state is safe to travel to and from")
      }
    }

    def deathMonthlyAvg(): Unit={
      //Average death per month println statement here:
      var sumAvg = 0
      print("Enter month to retrieve the average possible death information involving covid-19: \n>")
      var monthAvg = StdIn.readLine()//TODO I/O

      print("Enter year to retrieve the average possible death information involving covid-19: \n>")
      var yearAvg = StdIn.readLine()//TODO I/O

      print("Enter state name to retrieve the average possible death information involving covid-19: \n>")
      var stateNameAvg = StdIn.readLine()//TODO I/O
      var avgStateDeath = spark.sql(s"Select ${monthAvg}_${yearAvg} From CovidDeathsUs Where Combined_Key like '%, $stateNameAvg, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      val avgStateDeathCount = avgStateDeath.count.toInt
      val avgState1 = avgStateDeath.take(avgStateDeathCount)

      for(i<-0 to avgStateDeathCount-1){
        sumAvg = sumAvg + avgState1(i).getString(0).toInt
      }

      val averageStateMonth = sumAvg.toFloat/avgStateDeathCount
      println(s"The state of $stateNameAvg in the month of $monthAvg $yearAvg was on average $averageStateMonth of possible Covid-19 Deaths")
    }

    def springOrSummerTravel(): Unit = {
      print("Please select the state you want to compare for the spring and summer months of 2020: \n>")
      var springSummerState = StdIn.readLine()//TODO I/O
      //Summer(June, July, August) of 2020 because only in 2020 are these months listed month deaths if need be is here:
      var summerTotal = 0
      var summerMonthDFJune = spark.sql(s"Select June_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      var summerMonthDFJuly = spark.sql(s"Select July_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      var summerMonthDFAugust = spark.sql(s"Select August_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      val summerMonthCountJune = summerMonthDFJune.count.toInt
      var summer1 = summerMonthDFJune.take(summerMonthCountJune)
      val summerMonthCountJuly = summerMonthDFJuly.count.toInt
      var summer2 = summerMonthDFJuly.take(summerMonthCountJuly)
      val summerMonthCountAugust = summerMonthDFAugust.count.toInt
      var summer3 = summerMonthDFAugust.take(summerMonthCountAugust)

      for(i<-0 to summerMonthCountJune-1){
        summerTotal = summerTotal + summer1(i).getString(0).toInt
      }
      for(i<-0 to summerMonthCountJuly-1){
        summerTotal = summerTotal + summer2(i).getString(0).toInt
      }
      for(i<-0 to summerMonthCountAugust-1){
        summerTotal = summerTotal + summer3(i).getString(0).toInt
      }

      //Spring month(March, April, May) of 2020 deaths if need be is here:
      var springTotal = 0
      var springMonthMarchDF = spark.sql(s"Select March_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      var springMonthAprilDF = spark.sql(s"Select April_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      var springMonthMayDF = spark.sql(s"Select May_2020 From CovidDeathsUS Where Combined_Key like '%, $springSummerState, US' and Combined_Key not like 'Out of%' and Combined_Key not like 'Unassigned%'")
      val springMonthCountMarch = springMonthMarchDF.count.toInt
      var spring1 = springMonthMarchDF.take(springMonthCountMarch)
      val springMonthCountApril = springMonthAprilDF.count.toInt
      var spring2 = springMonthAprilDF.take(springMonthCountApril)
      val springMonthCountMay = springMonthMayDF.count.toInt
      var spring3 = springMonthMayDF.take(springMonthCountMay)

      for(i<-0 to springMonthCountMarch-1){
        springTotal = springTotal + spring1(i).getString(0).toInt
      }
      for(i<-0 to springMonthCountApril-1){
        springTotal = springTotal + spring2(i).getString(0).toInt
      }
      for(i<-0 to springMonthCountMay-1){
        springTotal = springTotal + spring3(i).getString(0).toInt
      }

      //If statement to determine which months summer or spring months is higher is here:
      var summerMonthsMore = summerTotal - springTotal
      var springMonthsMore = springTotal - summerTotal

      if(summerTotal > springTotal){
        println(s"It's safer to travel in the spring there are $summerMonthsMore more people who have died in the summer")
      }
      else{
        println(s"It's safer to travel in the summer there are $springMonthsMore more people who have died in the spring")
      }
    }

    def closeDeathTable(): Unit ={
      spark.close()
    }
  }
}
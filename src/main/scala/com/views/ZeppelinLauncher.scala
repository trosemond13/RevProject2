package com.views

import org.apache.zeppelin.client.{ClientConfig, ZeppelinClient}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.DataFrame
import scala.io.StdIn

object ZeppelinLauncher {
  def createNote(): Unit = {
    val clientConfig = new ClientConfig("http://localhost:8080")
    val zClient = new ZeppelinClient(clientConfig)
    val notePath = "/notebook/ZepTest"
    val noteId = zClient.createNote(notePath)

    try {
      val paragraphId = zClient.addParagraph(noteId, "Test Note", "Test")
      val addQuery = zClient.executeParagraph(noteId, paragraphId)

    } finally {
      // you need to stop interpreter explicitly if you are running paragraph separately.
      zClient.stopInterpreter(noteId, "spark")
    }
  }
}

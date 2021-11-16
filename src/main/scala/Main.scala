import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.DigestUtils
import java.nio.charset.StandardCharsets

import scala.annotation.tailrec
import scala.sys.exit

// This is a comment for the main file
object Main {

  def main(args:Array[String]): Unit = {
    UserDB.connect()
    Hive.connect()
    userMainMenu()
  }

  def userMainMenu() : Unit = {
    println("==== Main Menu ====")
    println("(1) NFL Data Menu")
    println("(2) Exit")
    val option = scala.io.StdIn.readLine()
    option match {
      case "1" => dataMenu()
      case "2" => exitApp()
      case _ => userMainMenu()
    }
    userMainMenu()
  }

  def dataMenu() : Unit = {
    println("NFL Data Totals for 2021-2022 Season")
    println("(1) Total Sacks")
    println("(2) Total Rushing Yards")
    println("(3) Total Penalty Yards")
    println("(4) Total run plays through the right guard position")
    println("(5) Total plays in ShotGun")
    println("(6) How many yards will the 49ers gain on a single run play for Week 10?")
    println("(7) Return to Main")
    println("(8) Logout")
    println("(9) Exit")
    val option = scala.io.StdIn.readLine()
    option match {
      case "1" => Hive.showData(option)
      case "2" => Hive.showData(option)
      case "3" => Hive.showData(option)
      case "4" => Hive.showData(option)
      case "5" => Hive.showData(option)
      case "6" => Hive.showData(option)
      case "7" => userMainMenu()
      case "8" => userMainMenu()
      case "9" => exitApp()
      case _ => dataMenu()
    }
    dataMenu()
  }


  def exitApp() : Unit = {
    UserDB.closeConnection()
    println("Exiting...")
    exit(0)
  }

}

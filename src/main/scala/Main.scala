import java.beans.Statement
import java.sql.{Connection, DriverManager, SQLException}
import scala.io.StdIn
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions.{col, count, countDistinct, desc, when}
import CustomImplicits._
import java.security.MessageDigest

import scala.annotation.tailrec
import scala.sys.exit


object Main {

  def main(args:Array[String]): Unit = {
    var valid_cred = false
    var user = ""
    var pass = ""
    var encryptedpass = ""
    var privileges = ""
    var mainmenuselection = ""
    var mainmenucheck = false
    var optionmenuselection = ""
    var optionmenucheck = false
    var programexitcheck = false

    //CONNECT TO DATABASE TO GET USERS LOGIN INFO//
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = System.getenv("JDBC_URL")
    val username = System.getenv("JDBC_USER")
    val password = System.getenv("JDBC_PASSWORD")

    var connection: Connection = DriverManager.getConnection(url, username, password)
    val statement = connection.createStatement()

    //CONNECT TO SPARK AND CREATE A TABLE FOR SQL-LIKE QUERIES IN SPARK//
    //Hive.connect()
    //READS THE OLD(CURRENT) nfl_data2.csv TO CREATE A TABLE
    //COMMENTED OUT TO AVOID BREAKING THE CODE IF THE NEW FILE IS USED
    //UNCOMMENT IF THE TABLE CREATION HAS BEEN UPDATED TO INCLUDE THE NEW COLUMNS IN THE NEW FILE

    //INITIATE SPARK SESSION//
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sparkContext.setLogLevel("ERROR")

    //P2 ADDITION////P2 ADDITION////P2 ADDITION////P2 ADDITION//
    //CREATE BROADCAST VARIABLE, DATAFRAME, DATASET, AND RDD TO USE THROUGHOUT THE PROGRAM//

    //DATAFRAME//
    val nfldf = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("input/nfl_data2.csv")
    val reparnfldf = nfldf.repartition(3).toDF()
    reparnfldf.persist(StorageLevel.MEMORY_AND_DISK)
    //DATASET//
    val nflds = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("input/nfl_data2.csv").as[NFL]
    val reparnflds = nflds.repartition(3)
    reparnflds.persist(StorageLevel.MEMORY_AND_DISK)
    //RDD//
    val nflrdd = nfldf.rdd
    val reparnflrdd = nflrdd.repartition(3)
    reparnflrdd.persist(StorageLevel.MEMORY_AND_DISK)
    //BROADCAST VARIABLE (DATAFRAME)//
    val broadcastData = spark.sparkContext.broadcast(reparnfldf)


    //P2 ADDITION////P2 ADDITION////P2 ADDITION////P2 ADDITION//

    //PROGRAM LOOP
    do {

      valid_cred = false
      user = ""
      pass = ""
      encryptedpass = ""
      mainmenuselection = ""
      mainmenucheck = false
      optionmenuselection = ""
      optionmenucheck = false
      programexitcheck = false

      //MAIN MENU//
      do {
        println("Welcome to the main menu, please make a selection")
        println("1) Sign In")
        println("2) Exit")
        print("> ")
        mainmenuselection = StdIn.readLine()
        println()

        mainmenuselection match {
          case "1" =>
            //USER SIGN IN//
            do {
              print("Enter your username: ")
              user = StdIn.readLine()
              print("Enter your password: ")
              pass = StdIn.readLine()
              encryptedpass = md5(pass)
              println()

              var resultSet = statement.executeQuery("SELECT * FROM users;")
              resultSet.next()

              breakable {
                do {
                  var check_user = resultSet.getString(2)
                  var check_pass = resultSet.getString(3)
                  privileges = resultSet.getString(4)

                  if (check_user == user && check_pass == encryptedpass) {
                    valid_cred = true
                    mainmenucheck = true
                    println("Success! Welcome '" + user + "' with '" + privileges + "' privileges")
                    println()
                    break
                  }

                } while (resultSet.next())

                if (valid_cred == false) {
                  println(Console.RED + "ERROR: INCORRECT USER OR PASSWORD, TRY AGAIN" + Console.RESET)
                  println()
                }

              }

            } while (!valid_cred)

          case "2" =>
            mainmenucheck = true
            programexitcheck = true

          case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)

        }

      } while (!mainmenucheck)


      if (valid_cred) {

        //OPTION MENU
        do {
          println("What would you like to do today, " + user + "?")
          println("1) Most Popular Formations")
          println("2) Total Sacks")
          println("3) Query 3")
          println("4) Query 4")
          println("5) Query 5")
          println("6) Query 6")
          if(privileges == "admin"){
            println("7) Add user -ADMIN ONLY OPTION-")
            println("8) Delete user -ADMIN ONLY OPTION")
          }
          println("9) Back")
          print("> ")
          optionmenuselection = StdIn.readLine()
          println()

          if((optionmenuselection == "2" || optionmenuselection == "3") && privileges != "admin"){
            println(Console.YELLOW + "WARNING: ADMIN PRIVILEGE REQUIRED TO ACCESS THIS FEATURES" + Console.RESET)
            println()
            optionmenuselection = "thisdoesnothing"
          }

          optionmenuselection match {
            case "1" =>
              //QUERY 1
              //QUERY FORM: SELECT DISTINCT Formation, COUNT(Formation) OVER(Partition by Formation) AS Total_Times_Seen FROM nfl_data ORDER BY Total_Times_Seen DESC
              broadcastData.value.groupBy("Formation").count().sort(desc("count")).withColumnRenamed("count","Total_Times_Seen").show()

            case "2" =>
              //QUERY 2
              //QUERY FORM: SELECT count(isSack) as Total_Sacks FROM nfl_data WHERE isSack = 1
              nflds.groupBy("isSack").count().filter("isSack == 1").withColumnRenamed("count","Total_Sacks").show()

            case "3" =>
              //QUERY 3

            case "4" =>
              //QUERY 4

            case "5" =>
              //QUERY 5

            case "6" =>
              //QUERY 6


            case "7" =>
              //ADD A USER
              print("What is the name of the new user?: ")
              var newuser = StdIn.readLine()
              print("What is the password for this user?: ")
              var newpass = StdIn.readLine()
              var newencryptedpass = md5(newpass)

              var newpriv = ""

              breakable {
                do {
                  println("Would like to give this user admin or basic privileges?: ")
                  println("1) admin")
                  println("2) basic")
                  print("> ")
                  newpriv = StdIn.readLine()

                  newpriv match {
                    case "1" =>
                      newpriv = "admin"
                      break;
                    case "2" =>
                      newpriv = "basic"
                      break;
                    case _ =>
                      println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
                      println()
                  }

                } while (true)

              }

              statement.executeUpdate("INSERT INTO users (user_name,user_password,user_privileges) \n" +
                "VALUES ('" + newuser + "','" + newencryptedpass + "','" + newpriv + "');")

              println(Console.BLUE + "SUCCESS! USER HAS BEEN ADDED!" + Console.RESET)
              println()

            case "8" =>
              //DELETE A USER
              print("What is the User ID of the user you are trying to delete? ")
              var userid = StdIn.readLine()
              println()

              var sql = "SELECT * FROM users WHERE user_id = " + userid + ";"
              var resultSet = statement.executeQuery(sql)

              if(resultSet.next() == false){
                println(Console.YELLOW + "THIS USER DOES NOT EXISTS" + Console.RESET)
                println()
              }
              else{
                statement.executeUpdate("DELETE FROM users WHERE user_id = " + userid + ";")
                println(Console.BLUE + "USER DELETED SUCCESSFULLY" + Console.RESET)
                println()
              }

            case "9" => optionmenucheck = true
            case "thisdoesnothing" =>
            case _ => println(Console.RED + "ERROR, UNEXPECTED COMMAND: select a valid command from the selection menu" + Console.RESET)
              println()
          }

        } while (!optionmenucheck)


      }

    } while(!programexitcheck)

    println("Thank you for using OUR app, Goodbye!")

  }


  def md5(s: String):String = {
    var news=MessageDigest.getInstance("MD5").digest(s.getBytes)
    var news2 = news.mkString("")
    return news2
  }



}


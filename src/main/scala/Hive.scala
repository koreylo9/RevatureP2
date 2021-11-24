import org.apache.spark.sql.{SQLContext, SparkSession, functions}
import org.apache.spark.sql.SQLImplicits

case class nflD(name:String, rush:Int, direction:String)

// This is the Hive application that will take nfl data
// and put it into a Table and execute Spark Queries on them
object Hive {
  def main(args:Array[String]) : Unit = {
    connect()
    showData("1")
  }
  private var spark:SparkSession = _
  def connect() : Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    spark = SparkSession
      .builder()
      .appName("NFL DATA")
      .config("spark.master","local")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("DROP table IF EXISTS nfl_data")
    spark.sql("CREATE table IF NOT exists nfl_data(GameId int, GameDate Date, Quarter int, Minute int, Second int, OffenseTeam String," +
      "DefenseTeam String, Down int, ToGo int, YardLine int, SeriesFirstDown int, NextScore int, Description String, TeamWin int, SeasonYear int, Yards int," +
      "Formation String, PlayType String, IsRush int, IsPass int, IsIncomplete int, IsTouchdown int, PassType String, IsSack int, IsChallenge int, IsChallengeReversed int," +
      "IsMeasurement int, IsInterception int, IsFumble int, IsPenalty int, IsTwoPointConversion int, IsTwoPointConversionSuccessful int, RushDirection String, YardLineFixed int, YardLineDirection String," +
      "IsPenaltyAccepted int, PenaltyTeam String, IsNoPlay int, PenaltyType String, PenaltyYards int)" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")

          spark.sql("Load data Local Inpath 'nfldata_updated.csv' into table nfl_data")
//        spark.sql("select * from nfl_data ").show(100)


    //    spark.sql("CREATE table IF NOT exists nfl_data(GameId int, GameDate Date, OffenseTeam String," +
//      "DefenseTeam String, Description String,SeasonYear int, Yards int, Formation String, IsRush int," +
//      "IsPass int, IsSack int, IsPenalty int, RushDirection String, PenaltyYards int)" +
//      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")
//
//    spark.sql("Load data Local Inpath 'nfl_data2.csv' into table nfl_data")
    //    spark.sql("select * from nfl_data ").show(100)
  }





  def showData(choice:String) : Unit = {
//    choice match {
//      case "1" => spark.sql("SELECT count(isSack) as Total_Sacks " +
//        "FROM nfl_data WHERE isSack = 1").show()
//      case "2" => spark.sql("SELECT sum(yards) as Total_Rushing_Yards " +
//        "FROM nfl_data").show()
//      case "3" =>  spark.sql("SELECT sum(penaltyYards) as Total_Penalty_Yards " +
//        "FROM nfl_data").show()
//      case "4" => spark.sql("SELECT count(isRush) as Run_Plays_Right_Guard " +
//        "FROM nfl_data WHERE isRush = 1 AND rushDirection = 'RIGHT GUARD'").show()
//      case "5" => spark.sql("SELECT count(formation) as Total_Shotgun_Plays " +
//        "FROM nfl_Data WHERE formation = 'SHOTGUN'").show()
//      case "6" => spark.sql("SELECT ROUND(m.count/r.count,1) as YPR FROM " +
//        "(SELECT count(isRush) count FROM nfl_data WHERE isRush = 1 AND OffenseTeam = 'SF') r, " +
//        "(SELECT sum(yards) count FROM nfl_data WHERE isRush = 1 AND OffenseTeam = 'SF') m ").show()
//      case "7" => spark.sql("select * from nfl_data ").show(50)
//      case _ => println("No Results")
//    }

    //Angel Code
//    val nfldf = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("nfl_data2.csv")
//    val broadcastData = spark.sparkContext.broadcast(nfldf).value

    val nflupdated = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("nfldata_updated.csv")
    val broadcastDataNoRepar = spark.sparkContext.broadcast(nflupdated)

//    val fourthDownSuccess = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
//      (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")))
//    val fourthDownCntSuccess = fourthDownSuccess.agg(functions.count("Down")).first.getLong(0)

    val fourthDownSuccess10 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && (broadcastDataNoRepar.value("ToGo") >= 9 ))
    val fourthDownCntSuccess10 = fourthDownSuccess10.agg(functions.count("Down")).first.getLong(0)


    val fourthDownSuccess8 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") <= 8) && (broadcastDataNoRepar.value("ToGo") > 6)))
    val fourthDownCntSuccess8 = fourthDownSuccess8.agg(functions.count("Down")).first.getLong(0)

    val fourthDownSuccess6 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 6) || (broadcastDataNoRepar.value("ToGo") === 5)))
    val fourthDownCntSuccess6 = fourthDownSuccess6.agg(functions.count("Down")).first.getLong(0)

    val fourthDownSuccess4 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 4) || (broadcastDataNoRepar.value("ToGo") === 3)))
    val fourthDownCntSuccess4 = fourthDownSuccess4.agg(functions.count("Down")).first.getLong(0)

    val fourthDownSuccess2 = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      ((broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1)) && (broadcastDataNoRepar.value("Yards") >= broadcastDataNoRepar.value("ToGo")) && ((broadcastDataNoRepar.value("ToGo") === 2) || (broadcastDataNoRepar.value("ToGo") === 1)))
    val fourthDownCntSuccess2 = fourthDownSuccess2.agg(functions.count("Down")).first.getLong(0)

    val fourthDownTotal = broadcastDataNoRepar.value.select("Down","isRush","isPass").filter((broadcastDataNoRepar.value("Down") === 4) &&
      (broadcastDataNoRepar.value("isRush") === 1 || broadcastDataNoRepar.value("isPass") === 1))
    val fourthDownCntTotal = fourthDownTotal.agg(functions.count("Down")).first.getLong(0)

    val successRate10 = (fourthDownCntSuccess10.toDouble/fourthDownCntTotal.toDouble) *100.0
    val successRate8 = (fourthDownCntSuccess8.toDouble/fourthDownCntTotal.toDouble) *100.0
    val successRate6 = (fourthDownCntSuccess6.toDouble/fourthDownCntTotal.toDouble) *100.0
    val successRate4 = (fourthDownCntSuccess4.toDouble/fourthDownCntTotal.toDouble) *100.0
    val successRate2 = (fourthDownCntSuccess2.toDouble/fourthDownCntTotal.toDouble) *100.0

    println("4th Down Plays Success Rate 9 yards or more: " + f"$successRate10%1.2f" + "%")
    println("4th Down Plays Success Rate 7-8 yards: " + f"$successRate8%1.2f" + "%")
    println("4th Down Plays Success Rate 5-6 yards: " + f"$successRate6%1.2f" + "%")
    println("4th Down Plays Success Rate 3-4 yards: " + f"$successRate4%1.2f" + "%")
    println("4th Down Plays Success Rate 1-2 yards: " + f"$successRate2%1.2f" + "%")







    //Dataframe
//    val rdd = spark.table("nfl_data")

    // DataFrame to DataSet

    // Left End Yards Sum
    val rdLeftEnd = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT END")
    val sumLeftEnd = rdLeftEnd.agg(functions.sum("yards")).first.get(0)


    //Right End Yards Sum
    val rdRightEnd = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT END")
    val sumRightEnd = rdRightEnd.agg(functions.sum("yards")).first.get(0)


    //Left Guard Yards Sum
    val rdLeftGuard = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT GUARD")
    val sumLeftGuard = rdLeftGuard.agg(functions.sum("yards")).first.get(0)


    //Right Guard Yards Sum
    val rdRightGuard = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT GUARD")
    val sumRightGuard = rdRightGuard.agg(functions.sum("yards")).first.get(0)


    //Left Tackle Yards Sum
    val rdLeftTackle = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "LEFT TACKLE")
    val sumLeftTackle = rdLeftTackle.agg(functions.sum("yards")).first.get(0)

    //Right Tackle Yards Sum
    val rdRightTackle = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "RIGHT TACKLE")
    val sumRightTackle = rdRightTackle.agg(functions.sum("yards")).first.get(0)

    //Center Yards Sum
    val rdCenter = broadcastDataNoRepar.value.select("offenseteam","yards").filter((broadcastDataNoRepar.value("isRush") === 1) &&
      broadcastDataNoRepar.value("offenseTeam") === "SF" && broadcastDataNoRepar.value("rushdirection") === "CENTER")
    val sumCenter = rdCenter.agg(functions.sum("yards")).first.get(0)

    printf("%-15s%-15s%-15s%-15s%-15s%-15s%-15s%-15s\n","Name","Left End","Left Tackle","Left Guard","Center","Right Guard","Right Tackle","Right End")

    printf("%-15s%-15s%-15s%-15s%-15s%-15s%-15s%-15s\n","SF 49ers",sumLeftEnd,sumLeftTackle,sumLeftGuard,sumCenter,sumRightGuard,sumRightTackle,sumRightEnd)

//    spark.sql("Select * FROM " +
//      "(Select sum(yards) as Left_End FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'LEFT END') t1," +
//      "(Select sum(yards) as Left_Tackle FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'LEFT TACKLE') t2," +
//      "(Select sum(yards) as Left_Guard FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'LEFT GUARD') t3," +
//      "(Select sum(yards) as Center FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'CENTER') t4," +
//      "(Select sum(yards) as Right_Guard FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'RIGHT GUARD') t5," +
//      "(Select sum(yards) as Right_Tackle FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'RIGHT TACKLE') t6," +
//      "(Select sum(yards) as Right_End FROM nfl_data WHERE offenseTeam = 'SF' AND isRush = 1 AND rushdirection = 'RIGHT END') t7 ").show()





  }


}

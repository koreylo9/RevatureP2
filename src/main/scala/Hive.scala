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
    spark.sql("CREATE table IF NOT exists nfl_data(GameId int, GameDate Date, OffenseTeam String," +
      "DefenseTeam String, Description String,SeasonYear int, Yards int, Formation String, IsRush int," +
      "IsPass int, IsSack int, IsPenalty int, RushDirection String, PenaltyYards int)" +
      "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'")

    spark.sql("Load data Local Inpath 'nfl_data2.csv' into table nfl_data")
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
    val nfldf = spark.read.option("header","true").option("delimiter",",").option("inferSchema","true").csv("nfl_data2.csv")
    val broadcastData = spark.sparkContext.broadcast(nfldf).value

    //Dataframe
//    val rdd = spark.table("nfl_data")

    // DataFrame to DataSet
    val rds = broadcastData.select("offenseteam","yards").filter((broadcastData("isRush") === 1) &&
      broadcastData("offenseTeam") === "SF" && broadcastData("rushdirection") === "LEFT END")


    val rd2 = broadcastData.select("offenseteam","yards").filter((broadcastData("isRush") === 1) &&
      broadcastData("offenseTeam") === "SF" && broadcastData("rushdirection") === "RIGHT END")

    rds.withColumnRenamed("yards","rushLeft").join(rd2.withColumnRenamed("yards","rushRight"),"offenseteam").show()

//    rds.join(rd2,"offenseteam").show()


//     Perform action
//    val rda = rds.agg(functions.sum("yards")).first.get(0)

    //Test against sql query
//    val dataF = spark.sql("SELECT OffenseTeam,isRush,rushdirection FROM nfl_data WHERE isRush = 1 AND OffenseTeam = 'SF' AND rushdirection='LEFT END'").show()



    //result that should be same as query
//    println(rda)



  }


}

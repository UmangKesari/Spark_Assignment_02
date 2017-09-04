import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrameApplication extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Demo")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val csvSqlRead =sparkSession.read.option("header", "true").option("inferSchema","true").
    csv("/home/knoldus/Downloads/Spark_Assignment_02/src/main/resources/D1.csv")

  csvSqlRead.createOrReplaceTempView("BundesligaTable")

  val sqlDF = sparkSession.sql("SELECT * FROM BundesligaTable LIMIT 5")
  sqlDF.show()

  val matchesPlayedByTeam =
    csvSqlRead.groupBy("HomeTeam").count().union(csvSqlRead.select("AwayTeam")
      .except(csvSqlRead.select("HomeTeam")).withColumn("count", lit(0)))

  matchesPlayedByTeam.show(false)


  sparkSession.sql("SELECT HomeTeam, count(HomeTeam) as home_matches_count FROM FootballMatches GROUP BY HomeTeam").
    createOrReplaceTempView("HomeMatches")

  sparkSession.sql("SELECT AwayTeam, count(AwayTeam) as away_matches_count FROM BundesligaTable GROUP BY AwayTeam").
    createOrReplaceTempView("AwayMatches")

  sparkSession.sql("SELECT HomeTeam, count(HomeTeam) as home_wins_count FROM BundesligaTable WHERE FTR = 'H' GROUP BY HomeTeam").
    createOrReplaceTempView("WonAtHome")

  sparkSession.sql("SELECT AwayTeam, count(AwayTeam) as away_wins_count FROM BundesligaTable WHERE FTR = 'A' GROUP BY AwayTeam").
    createOrReplaceTempView("WonAtForeign")

  sparkSession.sql("SELECT WonAtHome.HomeTeam as team, (WonAtHome.home_wins_count + WonAtForeign.away_wins_count) as won FROM WonAtHome INNER JOIN WonAtForeign ON WonAtHome.HomeTeam = WonAtForeign.AwayTeam").
    createOrReplaceTempView("TotalWins")

  sparkSession.sql("SELECT HomeMatches.HomeTeam as team, (HomeMatches.home_matches_count + AwayMatches.away_matches_count) as played FROM HomeMatches INNER JOIN AwayMatches ON HomeMatches.HomeTeam = AwayMatches.AwayTeam").
    createOrReplaceTempView("TotalMatchesPlayed")

  sparkSession.sql("SELECT TotalWins.team, (TotalWins.won / TotalMatchesPlayed.played)*100 as percentage FROM TotalWins INNER JOIN TotalMatchesPlayed ON TotalWins.team = TotalMatchesPlayed.team LIMIT 10").
    show(false)

}

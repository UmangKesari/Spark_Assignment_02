import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._

case class Football(HomeTeam: String, AwayTeam: String, HomeTeamGoal: Int, AwayTeamGoal: Int, TeamWon: String)

object DataSetApplication extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Demo")
  val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  import sparkSession.implicits._

  val dataSet: Dataset[Football] = sparkSession.read.option("header", "true").option("inferSchema", "true").
    csv("/home/knoldus/Downloads/Spark_Assignment_02/src/main/resources/D1.csv").
    select($"HomeTeam", $"AwayTeam", $"FTHG", $"FTAG", $"FTR").as[Football]


  val matchesPlayed: Dataset[Row] = dataSet.select(col("HomeTeam")).union(dataSet.select(col("AwayTeam"))).groupBy(col("HomeTeam") as "Teams" ).count
  matchesPlayed.show(false)

  val topTenTeam = dataSet.filter(game => game.TeamWon == "H").groupBy(col("HomeTeam")).count
    .union(dataSet.filter(secGame => secGame.TeamWon == "A").groupBy(col("AwayTeam")).count).groupBy(col("HomeTeam"))
    .sum("count").sort(desc("sum(count)")).limit(10)
  topTenTeam.show(false)
}
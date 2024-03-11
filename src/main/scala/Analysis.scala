import java.sql.{Connection, DriverManager}
import scala.util.{Failure, Success, Try}

object UserAnalysisFromDatabase {
  case class UserAnalysisData(username: String, aadharCard: String, frequency: Int, totalUsers: Int)
  def main(args: Array[String]): Unit = {
    val result = Try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url="jdbc:mysql://localhost:3306/kafka"
      val user="root"
      val password = "*****"
      val connection = DriverManager.getConnection(url, user, password)
      val analysisData = performUserAnalysis(connection)
      printAnalysisResult(analysisData)
      connection.close()
    }
    result match{
      case Success(_) => println("User analysis completed successfully.")
      case Failure(exception) => println(s"An error occurred: ${exception.getMessage}")
    }
  }
  def performUserAnalysis(connection: Connection): UserAnalysisData = {
    val userDataList = fetchDataFromDatabase(connection)
    val userDataMap = userDataList.groupBy(identity).mapValues(_.size)
    val totalUsers = userDataMap.values.sum
    val repeatUserCount = userDataMap.count { case (_, count) => count > 1 }
    UserAnalysisData("Total", "Total", repeatUserCount, totalUsers)
  }
  def fetchDataFromDatabase(connection: Connection): List[(String, String)] = {
    val query =
      """
        |SELECT username, aadhar_card FROM UserDetails
        |""".stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(query)

    Iterator.continually((resultSet.next(), resultSet)).takeWhile(_._1).map { case (_, rs) =>
      (rs.getString("username"), rs.getString("aadhar_card"))
    }.toList
  }
  def printAnalysisResult(analysisData: UserAnalysisData): Unit = {
    val repeatPercentage = if (analysisData.totalUsers > 0) {
      (analysisData.frequency.toDouble/analysisData.totalUsers.toDouble) * 100
    } else {
      0.0
    }
    println(s"Total Users: ${analysisData.totalUsers}")
    println(s"Repeat Users: ${analysisData.frequency}")
    println(f"Repeat User Percentage: $repeatPercentage%.2f%%")
  }
}

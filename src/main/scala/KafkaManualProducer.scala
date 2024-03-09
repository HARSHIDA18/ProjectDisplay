import java.util.Properties
import java.util.Scanner
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.{Failure, Success, Try}
case class FlightData(aircraft_id: String, airlines_name: String, price: Double, destination: String, source: String)
object KafkaManualProducer {
  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "Flights"

    val url = "jdbc:mysql://localhost:3306/kafka"
    val user = "root"
    val password = "HShaily@20"
    val connection = java.sql.DriverManager.getConnection(url, user, password)
    val scanner = new Scanner(System.in)

    val result: Try[Unit] = Try {
      print("Enter airlines_name: ")
      val airlinesName = scala.io.StdIn.readLine()
      print("Enter price: ")
      val price = scala.io.StdIn.readDouble()
      print("Enter destination: ")
      val destination = scala.io.StdIn.readLine()
      print("Enter source: ")
      val source = scala.io.StdIn.readLine()
      val textData = s"$airlinesName,$price,$destination,$source"
      val record = new ProducerRecord[String, String](topic, textData)
      producer.send(record)
      val insertQuery =
        """
          |INSERT INTO Flight1(
          |  airlines_name, price, destination, source
          |) VALUES (?, ?, ?, ?)
          |""".stripMargin

      val preparedStatement = connection.prepareStatement(insertQuery)
      preparedStatement.setString(1, airlinesName)
      preparedStatement.setDouble(2, price)
      preparedStatement.setString(3, destination)
      preparedStatement.setString(4, source)
      preparedStatement.executeUpdate()
      connection.commit()
    }


    result match {
      case Success(_) =>
        println("Data successfully sent to Kafka and inserted into the database.")
      case Failure(exception) =>
        println(s"An error occurred: ${exception.getMessage}")
    }
    producer.close()
    connection.close()
    scanner.close()
  }
}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.sql.DriverManager
import java.util.{Properties, Scanner}
import scala.util.{Failure, Success, Try}


case class FlightData(flight_id: String, aircraft_id: String, airlines_name: String, price: Double, destination: String, source: String, departure_time: String, arrival_time: String, seat_availability: Int)
object KafkaManualProducer {
  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.cj.jdbc.Driver")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "FlightDet"

    val url = "jdbc:mysql://localhost:3306/kafka"
    val user = "root"
    val password = "*****"
    val connection = DriverManager.getConnection(url, user, password)
    val scanner = new Scanner(System.in)

    println("Enter data using Kafka producer")

    val result: Try[Unit] = Try {
      print("Enter aircraft_id: ")
      val aircraftId = scala.io.StdIn.readLine()
      print("Enter airlines_name: ")
      val airlinesName = scala.io.StdIn.readLine()
      print("Enter price: ")
      val price = scala.io.StdIn.readDouble()
      print("Enter destination: ")
      val destination = scala.io.StdIn.readLine()
      print("Enter source: ")
      val source = scala.io.StdIn.readLine()
      print("Enter departure_time: ")
      val departureTime = scala.io.StdIn.readLine()
      print("Enter arrival_time: ")
      val arrivalTime = scala.io.StdIn.readLine()
      print("Enter seat_availability: ")
      val seatAvailability = scala.io.StdIn.readInt()

      val record = new ProducerRecord[String, String](topic, s"$aircraftId,$airlinesName,$price,$destination,$source,$departureTime,$arrivalTime,$seatAvailability")
      producer.send(record)

      val insertQuery =
        s"""
           |INSERT INTO FlightDet1(
           |  aircraft_id, airlines_name, price, destination, source, departure_time, arrival_time, seat_availability
           |) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
           |""".stripMargin

      val preparedStatement = connection.prepareStatement(insertQuery)
      preparedStatement.setString(1, aircraftId)
      preparedStatement.setString(2, airlinesName)
      preparedStatement.setDouble(3, price)
      preparedStatement.setString(4, destination)
      preparedStatement.setString(5, source)
      preparedStatement.setString(6, departureTime)
      preparedStatement.setString(7, arrivalTime)
      preparedStatement.setInt(8, seatAvailability)

      preparedStatement.executeUpdate()
      connection.commit()
    }

    result match {
      case Success(_) =>
        println("Data successfully processed.")
      case Failure(exception) =>
        println(s"An error occurred: ${exception.getMessage}")
    }

    producer.close()
    connection.close()
    scanner.close()
  }
}

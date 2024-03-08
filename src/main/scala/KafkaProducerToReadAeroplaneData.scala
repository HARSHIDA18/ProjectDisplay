import java.sql.{Connection, DriverManager, PreparedStatement}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import play.api.libs.json._
object KafkaProducer {
  case class FlightData(aircraft_id: String,airlines_name: String,travel: TravelData,aircraftEmergency_phone: String, aircraft_rating: Double, price: PriceData, time: TimeData)
  case class TravelData(destination_city: String,source_city: String)
  case class PriceData(INR: Double,USD: Double)
  case class TimeData(departure_time: String,arrival_time: String)

  def main(args: Array[String]): Unit = {
    implicit val travelFormat: Format[TravelData] = Json.format[TravelData]
    implicit val priceFormat: Format[PriceData] = Json.format[PriceData]
    implicit val timeFormat: Format[TimeData] = Json.format[TimeData]
    implicit val flightDataFormat: Format[FlightData] = Json.format[FlightData]

    Class.forName("com.mysql.cj.jdbc.Driver")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "KafkaStreaming"
    val filePath = "G:\\FinalProjectShow\\src\\resources\\AeroplaneJSON.json"
    val lines = scala.io.Source.fromFile(filePath).getLines()
    val url = "jdbc:mysql://localhost:3306/Kafka"
    val user = "root"
    val password = "*****"
    val connection: Connection = DriverManager.getConnection(url, user, password)
    lines.zipWithIndex.foreach { case (line, index) =>
      try {
        val json = Json.parse(line.replace("'", "\""))
        val flightData = json.as[FlightData]
        val record = new ProducerRecord[String, String](topic, line)
        producer.send(record)
        val insertQuery =
          """
            |INSERT INTO kafka_aircraft_data (
            |  aircraft_id, airlines_name, destination_city, source_city,
            |  aircraftEmergency_phone, aircraft_rating, INR_price, USD_price,
            |  departure_time, arrival_time
            |) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            |""".stripMargin

        val preparedStatement: PreparedStatement = connection.prepareStatement(insertQuery)
        preparedStatement.setString(1, flightData.aircraft_id)
        preparedStatement.setString(2, flightData.airlines_name)
        preparedStatement.setString(3, flightData.travel.destination_city)
        preparedStatement.setString(4, flightData.travel.source_city)
        preparedStatement.setString(5, flightData.aircraftEmergency_phone)
        preparedStatement.setDouble(6, flightData.aircraft_rating)
        preparedStatement.setDouble(7, flightData.price.INR)
        preparedStatement.setDouble(8, flightData.price.USD)
        preparedStatement.setString(9, flightData.time.departure_time)
        preparedStatement.setString(10, flightData.time.arrival_time)

        preparedStatement.executeUpdate()
      } catch {
        case e: Exception =>
          println(s"Error at line $index: $line")
          e.printStackTrace()
      }
    }
  }
}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.sql.{Connection, DriverManager}
import java.util.{Properties, Scanner}
import scala.util.{Failure, Success, Try}
case class UserData(username:String,aadharCard:String,source:String,destination:String)
object UserBooking{
  def isValidAadharCard(aadharCard:String):Boolean={
    aadharCard.matches("\\d{12}")
  }
  def getAvailableFlights(source:String,destination:String,connection:Connection):List[FlightData]={
    val query =
      """
        |SELECT * FROM FlightDet1
        |WHERE source = ? AND destination = ?
        |""".stripMargin

    val preparedStatement=connection.prepareStatement(query)
    preparedStatement.setString(1, source)
    preparedStatement.setString(2, destination)

    val resultSet=preparedStatement.executeQuery()
    var flights:List[FlightData]=List()

    while (resultSet.next()) {
      val flight=FlightData(
        resultSet.getString("flight_id"),
        resultSet.getString("aircraft_id"),
        resultSet.getString("airlines_name"),
        resultSet.getDouble("price"),
        resultSet.getString("destination"),
        resultSet.getString("source"),
        resultSet.getString("departure_time"),
        resultSet.getString("arrival_time"),
        resultSet.getInt("seat_availability")
      )
      flights=flight::flights
    }
    flights.reverse
  }
  def showAvailableFlights(flights:List[FlightData]):Unit={
    println("Available Flights:")
    flights.foreach(println)
  }

  def bookAndDeductSeat(aircraftId:String,username:String,aadharCard:String,connection:Connection):Unit={
    val checkAvailabilityQuery=
      """
        |SELECT seat_availability FROM FlightDet1
        |WHERE aircraft_id = ? AND seat_availability > 0
        |""".stripMargin

    val updateSeatAvailabilityQuery=
      """
        |UPDATE FlightDet1
        |SET seat_availability = seat_availability - 1
        |WHERE aircraft_id = ? AND seat_availability > 0
        |""".stripMargin

    val insertBookingQuery=
      """
        |INSERT INTO UserDetails(username, aadhar_card, aircraft_id)
        |VALUES (?, ?, ?)
        |""".stripMargin

    val checkAvailabilityStatement=connection.prepareStatement(checkAvailabilityQuery)
    checkAvailabilityStatement.setString(1,aircraftId)
    val availabilityResultSet=checkAvailabilityStatement.executeQuery()
    if (availabilityResultSet.next()){
      val availableSeats = availabilityResultSet.getInt("seat_availability")
      if (availableSeats>0){
        val updateSeatStatement=connection.prepareStatement(updateSeatAvailabilityQuery)
        updateSeatStatement.setString(1,aircraftId)
        val insertBookingStatement=connection.prepareStatement(insertBookingQuery)
        insertBookingStatement.setString(1,username)
        insertBookingStatement.setString(2,aadharCard)
        insertBookingStatement.setString(3,aircraftId)
        connection.setAutoCommit(false)
        try{
          updateSeatStatement.executeUpdate()
          insertBookingStatement.executeUpdate()
          connection.commit()
        }catch{
          case e: Exception =>
            connection.rollback()
            throw e
        }finally{
          connection.setAutoCommit(true)
        }
      }else{
        println("No available seats for the selected flight.")
      }
    }else{
      println("Invalid flight selection.")
    }
  }
  def sendUserDataToKafka(userData:UserData,producer:KafkaProducer[String, String],topic:String):Unit={
    val record = new ProducerRecord[String,String](topic, s"${userData.username},${userData.aadharCard},${userData.source},${userData.destination}")
    producer.send(record)
  }
  def main(args: Array[String]):Unit ={
    val result=Try{
      Class.forName("com.mysql.cj.jdbc.Driver")
      val url="jdbc:mysql://localhost:3306/kafka"
      val user="root"
      val password="*****"
      val connection=DriverManager.getConnection(url,user,password)
      val kafkaProps=new Properties()
      kafkaProps.put("bootstrap.servers","localhost:9092")
      kafkaProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
      kafkaProps.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
      val producer=new KafkaProducer[String,String](kafkaProps)
      val topic = "UserData"
      val scanner = new Scanner(System.in)
      println("Enter your details:")
      print("Username: ")
      val username=scanner.nextLine()
      print("Aadhar Card Number: ")
      val aadharCard = scanner.nextLine()

      if (isValidAadharCard(aadharCard)) {
        print("Source: ")
        val source = scanner.nextLine()

        print("Destination: ")
        val destination = scanner.nextLine()
        println(s"Do you want to book a flight from $source to $destination? (YES/NO): ")
        val bookFlight = scanner.nextLine().toUpperCase()
        if (bookFlight == "YES") {
          val availableFlights = getAvailableFlights(source, destination, connection)
          if (availableFlights.nonEmpty) {
            showAvailableFlights(availableFlights)
            println("Enter the aircraft_id to book: ")
            val aircraftId = scanner.nextLine()
            bookAndDeductSeat(aircraftId, username, aadharCard, connection)
            sendUserDataToKafka(UserData(username, aadharCard, source, destination), producer, topic)
            println(s"Flight $aircraftId booked for $username")
          } else{
            println("No available flights for the selected source and destination.")
          }
        } else{
          println("Flight booking canceled.")
        }
        producer.close()
        connection.close()
        scanner.close()
      } else {
        println("Invalid Aadhar card number. Please enter a valid Aadhar card.")
      }
    }
    result match {
      case Success(_) => println("Booking completed successfully.")
      case Failure(exception) => println(s"An error occurred: ${exception.getMessage}")
    }
  }
}

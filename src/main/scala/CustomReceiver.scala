import java.sql.{Connection, DriverManager, ResultSet}

import org.apache.log4j._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class CustomReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart(): Unit = {
    Future {
      val logger = Logger.getLogger(this.getClass)
      logger.info("In the onStart method of CustomReceiver")
      val driver = "org.postgresql.Driver"
      val url = "jdbc:postgresql://localhost:5432/sparkstreaming"
      val username = "postgres"
      val password = "knoldus"
      val connection = try {
        logger.info("Defining connection")
        Class.forName(driver)
        DriverManager.getConnection(url, username, password)
      } catch {
        case e: Exception => throw new Exception("Connection was not successfull")
      }
      logger.info("Connection defined")
      val statement = connection.createStatement()
      logger.info("Statement defined")
      val data = statement.executeQuery("SELECT * FROM sparktable")//.getString("data")
      while(data.next())
        {
          store(data.getString("data"))
          logger.info("data = " + data.getString("data"))
        }
      connection.close()
      restart("Restarting onStart()")
    }
  }

  override def onStop(): Unit = {

  }
}

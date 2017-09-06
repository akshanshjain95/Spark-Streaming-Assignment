import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming extends App {

  Logger.getLogger("org").setLevel(Level.OFF)

  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark Tutorial")

//  val sparkContext = new SparkContext(sparkConf)

  //val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  val streamingContext = new StreamingContext(sparkConf, Seconds.apply(2))

  val lines: ReceiverInputDStream[String] = streamingContext.receiverStream(new CustomReceiver)

  val words: DStream[Int] = lines.map(_.split(" ").length)

  words.foreachRDD(a => println(a.collect().toList))

  streamingContext.start()
  streamingContext.awaitTermination()

}



import event_time_window.{SensorReading, env, kafkaSource}
import org.apache.flink.api.common.eventtime.WatermarkStrategy.forMonotonousTimestamps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import java.util.Properties

object KafkaEventTime extends App{

  // Define a case class to represent events
  case class Event(id: Int, timestamp: Long, value: Int)

  // Create a StreamExecutionEnvironment
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  // Set the time characteristic to event time
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.setParallelism(1)
  val kafkaProps = new Properties()
  kafkaProps.setProperty("bootstrap.servers","localhost:9092")
  kafkaProps.setProperty("group.id","flink-consumer-group")
  val kafkaConsumer = new FlinkKafkaConsumer[String]("flink-example", new SimpleStringSchema(),kafkaProps)

 // val events = env.fromSource(kafkaSource, forMonotonousTimestamps(), "Kafka Source")
    kafkaConsumer.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(1)) {
      override def extractTimestamp(element: String): Long = element.split(",")(1).toLong
    })
  val events=env.addSource(kafkaConsumer)
  println(events)
    val timedEvents= events.map(a=>a.split(",")).map(a=>Event(a(0).toInt,a(1).toLong,a(2).toInt))
  // Create a tumbling window of 1 second and count the events within each window
  val result = timedEvents
    .keyBy(_.id % 2) // partition the events by their id modulo 2
    .timeWindow(Time.seconds(10))
    .sum("value")
  println("3")
  // Print the results
  result.print()

  // Start the execution
  env.execute("Event Time Example")
}

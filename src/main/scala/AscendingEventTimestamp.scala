import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

case class Event(time: Long, value: String)

object AscendingEventTimestamp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000) // Set watermark interval to 1 second

    // Set up Kafka consumer properties
    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "flink-consumer")

    // Create Flink-Kafka consumer
    val kafkaConsumer = new FlinkKafkaConsumer[String]("flink-example", new SimpleStringSchema(), kafkaProps)
    val stream = env.addSource(kafkaConsumer).map(line => {
        val tokens = line.split(",")
        Event(tokens(0).toLong, tokens(1))
      })
      .assignAscendingTimestamps(_.time) // Use event timestamps for watermarking

    val windowedStream = stream
      .keyBy(_.value)
      .timeWindow(Time.seconds(10))

    val aggregatedStream = windowedStream
      .reduce((e1, e2) => Event(e1.time max e2.time, e1.value)) // Calculate max event time for the window

    val output = aggregatedStream
      .map(event => s"${event.value}: ${event.time}")
      .print()

    env.execute("Periodic watermarking example")
  }
}

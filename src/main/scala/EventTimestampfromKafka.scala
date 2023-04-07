import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import processing_time_window.lines
import org.apache.flink.api.common.serialization.SimpleStringSchema
import java.util.Properties

object EventTimestampfromKafka
{
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setAutoWatermarkInterval(5000L) // emit a watermark every 5 seconds

    val kafkaProps = new Properties()
    kafkaProps.setProperty("bootstrap.servers", "localhost:9092")
    kafkaProps.setProperty("group.id", "flink-consumer-group")

    val kafkaConsumer = new FlinkKafkaConsumer[String](
      "flink-example",
      new SimpleStringSchema(),
      kafkaProps)

    val stream = env.addSource(kafkaConsumer)
      .map(line => {
        val tokens = SensorReading(line.split(",")(0),line.split(",")(1).toLong,line.split(",")(2).toDouble)
        tokens
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
          override def extractTimestamp(element: SensorReading): Long = 1 //element.timestamp
        }
      )
    println("before trigger")
    val check= stream.keyBy(a => (a.temperature%2)).timeWindow(Time.seconds(10)).sum(2)
    // print the stream to the console
    check.print()

    // execute the program
    env.execute("Event Time Example")
  }
}


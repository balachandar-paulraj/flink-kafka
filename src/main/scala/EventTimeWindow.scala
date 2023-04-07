import event_time_window.SensorReading
import event_time_window.temp.assignTimestampsAndWatermarks
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.eventtime.WatermarkStrategy.forMonotonousTimestamps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
class SensorTimeAssigner
  extends BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(5)) {
  /** Extracts timestamp from SensorReading. */
  override def extractTimestamp(r: String): Long = r.split(",")(1).toLong
}



object EventTimeWindow extends App{

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  env.getConfig.setAutoWatermarkInterval(1000L)
  case class SensorReading(id: String, timestamp: Long,amount:Int)
  // Adding KafkaSource
  val kafkaSource = KafkaSource.builder().setBootstrapServers("localhost:9092").setTopics("flink-example")
    .setGroupId("flink-consumer-group")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema()).build()

  val lines = env.fromSource(kafkaSource, forMonotonousTimestamps(), "Kafka Source").assignTimestampsAndWatermarks(new SensorTimeAssigner)
   val temp: DataStream[SensorReading]= lines.map(a=>a.split(",")).map(
     a=>SensorReading(a(0),a(1).toLong,a(2).toInt)
   )


  //KeyedByWindows
  //lines.keyBy(a => a%2==0).window()
  val sumGroups = temp.keyBy(a => a.id).timeWindow(Time.seconds(1)).sum(2)

  // Printing to console what we have consumed
  sumGroups.print()

  env.execute("Read from Kafka")

}

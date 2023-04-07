import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object ProcessingTimeWindow extends App {

  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // Adding KafkaSource
  val kafkaSource = KafkaSource.builder().setBootstrapServers("localhost:9092").setTopics("flink-example")
    .setGroupId("flink-consumer-group")
    .setStartingOffsets(OffsetsInitializer.latest())
    .setValueOnlyDeserializer(new SimpleStringSchema()).build()

  val lines = env.fromSource(kafkaSource, WatermarkStrategy.forMonotonousTimestamps(), "Kafka Source").map(a=>a.toInt)//.assignTimestampsAndWatermarks()
  env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
  //env.getConfig.setAutoWatermarkInterval(1000L)

  //KeyedByWindows
  //lines.keyBy(a => a%2==0).window()
  val sumGroups = lines.keyBy(a => (a%2)).timeWindow(Time.seconds(3)).sum(0)

  // Printing to console what we have consumed
  sumGroups.print()

  env.execute("Read from Kafka")

}

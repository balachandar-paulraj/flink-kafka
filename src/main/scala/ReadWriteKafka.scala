import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._

object ReadWriteKafka extends App{
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  // Adding KafkaSource
  val kafkaSource = KafkaSource.builder().setBootstrapServers("localhost:9092").setTopics("flink-example")
                  .setGroupId("flink-consumer-group")
                  .setStartingOffsets(OffsetsInitializer.latest())
                  .setValueOnlyDeserializer(new SimpleStringSchema()).build()
  // Before sending Kafka we need to serialize our value
  val serializer = KafkaRecordSerializationSchema.builder().setValueSerializationSchema(new SimpleStringSchema()).setTopic("flink-example-out").build()
  val kafkaSink = KafkaSink.builder().setBootstrapServers("localhost:9092").setRecordSerializer(serializer).build()
  // Consuming from Kafka
  val lines = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source")

  // Printing to console what we have consumed
  lines.print()
  // Producing to kafka what we have just consumed. But to different topic.
  lines.sinkTo(kafkaSink)
  env.execute("Read from Kafka")

}

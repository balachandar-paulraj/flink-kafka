import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

case class SensorReading(id: String, timestamp: Long, temperature: Double)

object EventTimestampFromSource {
  def main(args: Array[String]): Unit = {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // create a stream of SensorReading events with timestamps and watermarks
    val sensorData: DataStream[SensorReading] = env
      .fromElements(
        SensorReading("sensor_1", 1615429700000L, 25.8),
        SensorReading("sensor_2", 1615429710000L, 26.3),
        SensorReading("sensor_1", 1615429720000L, 26.1),
        SensorReading("sensor_2", 1615429730000L, 26.7),
        SensorReading("sensor_1", 1615429740000L, 26.4),
        SensorReading("sensor_2", 1615429750000L, 26.9),
        SensorReading("sensor_1", 1615429760000L, 26.5),
        SensorReading("sensor_2", 1615429770000L, 27.1),
        SensorReading("sensor_1", 1615429780000L, 26.8),
        SensorReading("sensor_2", 1615429790000L, 27.3)
      )
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(10)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp
        }
      )

    //Adding new line to test Github access from Scala code
    
    // print the stream to the console
    sensorData.print()

    // execute the program
    env.execute("Event Time Example")
  }
}

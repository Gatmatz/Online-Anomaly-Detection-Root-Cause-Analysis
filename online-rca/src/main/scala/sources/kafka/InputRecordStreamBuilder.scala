package sources.kafka

import config.AppConfig
import models.InputRecord
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import utils.dimension.DimensionsBuilder

object InputRecordStreamBuilder {
  def buildInputRecordStream(
                               kafkaTopic: String,
                               env: StreamExecutionEnvironment,
                               parallelism: Int,
                               kafkaOffset: String = "earliest",
                               groupId: String = AppConfig.Kafka.GROUP_ID): DataStream[InputRecord] = {

    val inputOrdersStream: DataStream[InputRecord] = {
      val kafkaConsumer = GenericJsonKafkaConsumer(kafkaTopic, groupId)

      val kafkaConsumerWithOffset = kafkaOffset.toLowerCase match {
        case "earliest" => kafkaConsumer.setStartFromEarliest()
        case "latest" => kafkaConsumer.setStartFromLatest()
        case t => kafkaConsumer.setStartFromTimestamp(t.toLong)
        //case _ => throw new IllegalArgumentException("kafkaOffset can either be earliest, latest or a timestamp")
      }
      env.addSource(kafkaConsumerWithOffset)
        .setParallelism(parallelism)
        .map(record => buildInputRecord(record))
    }
    inputOrdersStream
  }

  private def buildInputRecord(record: ObjectNode): InputRecord = {
    InputRecord(
      timestamp = record.get("value").get(AppConfig.InputStream.TIMESTAMP_FIELD).textValue(),
      value = record.get("value").get(AppConfig.InputStream.VALUE_FIELD).doubleValue(),
      dimensions = DimensionsBuilder.buildDimensionsMap(record)
    )
  }
}

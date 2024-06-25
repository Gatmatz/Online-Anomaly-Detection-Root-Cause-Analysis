package benchmarks

import anomaly_detection.detectors.{EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec}
import config.AppConfig
import models.{AnomalyEvent, InputRecord}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.junit.Test
import sources.kafka.InputRecordStreamBuilder

class AnomalyBenchmark {

  @Test
  def countAnomalies(): Unit = {
    // Input Stream Spec
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)

    val parallelism: Int = 1
    env.setParallelism(parallelism)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 20000 // How big the sample should be
    anomalySpec.warmupCount = 5000
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 19000
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 19000
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.999

    // Anomaly Detection
    val anomalyDetector = new EWAppxPercentileOutlierClassifier
    anomalyDetector.init(anomalySpec)

    // Input Stream Initialization
    val dataStream: DataStream[InputRecord] = InputRecordStreamBuilder
      .buildInputRecordStream(env)

    val anomalyEventStream: DataStream[AnomalyEvent] = anomalyDetector.runDetection(dataStream)

//    anomalyEventStream.print()
    val anomalies = anomalyEventStream.filter(_.isOutlier == true)

//    println(LocalTime.now())
    val countStream = anomalies
      .map(new MapFunction[AnomalyEvent, Long] {
        override def map(value: AnomalyEvent): Long = 1L
      })
      .keyBy(_ => 0)
      .reduce(_ + _)

    countStream.print()

    env.execute("Count Stream")
  }
}

package benchmarks

import anomaly_detection.detectors.{EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec}
import config.AppConfig
import models.{AnomalyEvent, InputRecord, RCAResult}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test
import root_cause_analysis.{EWStreamingSummarizer, EWStreamingSummarizerSpec}
import sinks.kafka.RCAResultJsonProducer
import sources.kafka.InputRecordStreamBuilder

import java.time.LocalTime

class MacrobaseBenchmark {

  @Test
  def testFromKafka(): Unit = {
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

    // Root Cause Analysis Spec
    val attributes = AppConfig.InputStream.DIMENSION_NAMES
    val summarizerSpec = EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 10000,
      decayType = "TUPLE_BASED",
      decayRate = 0.1,
      outlierItemSummarySize = 10000,
      inlierItemSummarySize = 10000,
      minOIRatio = 1,
      minSupport = 0.01,
      attributes = attributes,
      attributeCombinations = true,
      summaryGenerationPeriod = 10000
    )

    // Anomaly Detection
    val anomalyDetector = new EWAppxPercentileOutlierClassifier
    anomalyDetector.init(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec)
    println(LocalTime.now())
    // Input Stream Initialization
    val dataStream: DataStream[InputRecord] = InputRecordStreamBuilder
      .buildInputRecordStream(env)

    val anomalyEventStream: DataStream[AnomalyEvent] = anomalyDetector.runDetection(dataStream)

    // Root Cause Analysis
    summarizer
      .runSearch(anomalyEventStream)
      .addSink(RCAResultJsonProducer())

    env.execute("Kafka Benchmark Test")
  }
}

package jobs

import anomaly_detection.detectors.{EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec, ThresholdDetector, ThresholdDetectorSpec}
import config.AppConfig
import models.{AnomalyEvent, InputRecord}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import root_cause_analysis.{EWStreamingSummarizer, EWStreamingSummarizerSpec, HierarchicalContributorsFinder, SimpleContributorsFinder}
import sinks.kafka.RCAResultJsonProducer
import sources.kafka.InputRecordStreamBuilder

object MainJob {
  def main(args: Array[String]) {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)

    // Parse program arguments
    val parameters = ParameterTool.fromArgs(args)
    env.getConfig.setGlobalJobParameters(parameters)


    // load input stream
    val inputStream: DataStream[InputRecord] = InputRecordStreamBuilder.buildInputRecordStream(env)

    val spec = {
      if (AppConfig.AnomalyDetection.METHOD == "threshold") {
        val spec: ThresholdDetectorSpec = new ThresholdDetectorSpec()

        spec.min = 400.0f
        spec.max = 5000.0f

        spec
      }
      else if (AppConfig.AnomalyDetection.METHOD == "macrobase") {
          val spec: EWAppxPercentileOutlierClassifierSpec = new EWAppxPercentileOutlierClassifierSpec()

          spec.warmupCount = 100
          spec.sampleSize = 1000
          spec.decayPeriodType = "TUPLE_BASED"
          spec.decayPeriod = 10
          spec.decayRate = 0.01
          spec.trainingPeriodType = "TUPLE_BASED"
          spec.trainingPeriod = 10
          spec.percentile = 0.9

        spec
      }
      else {
        // ThresholdDetector is the default
        val spec: ThresholdDetectorSpec = new ThresholdDetectorSpec()

        spec.min = 3000.0f
        spec.max = 5000.0f

        spec
      }
    }

    val detector = {
      if (AppConfig.AnomalyDetection.METHOD == "threshold") {
        val detector: ThresholdDetector = new ThresholdDetector()
        detector.init(spec.asInstanceOf[ThresholdDetectorSpec])

        detector
      }
      else if (AppConfig.AnomalyDetection.METHOD == "macrobase")
        {
          val detector: EWAppxPercentileOutlierClassifier = new EWAppxPercentileOutlierClassifier()
          detector.init(spec.asInstanceOf[EWAppxPercentileOutlierClassifierSpec])

          detector
        }
      else {
        // ThresholdDetector is the default
        val detector: ThresholdDetector = new ThresholdDetector()
        detector.init(spec.asInstanceOf[ThresholdDetectorSpec])

        detector
      }
    }

    val anomaliesStream: DataStream[AnomalyEvent] = detector.runDetection(inputStream)

    // apply contributors finder
    val finder = {
      if (AppConfig.RootCauseAnalysis.METHOD == "hierarchical") {
        new HierarchicalContributorsFinder().runSearch(anomaliesStream).addSink(RCAResultJsonProducer())
      }
      else if (AppConfig.RootCauseAnalysis.METHOD == "simple") {
        new SimpleContributorsFinder().runSearch(anomaliesStream).addSink(RCAResultJsonProducer())
      }
      else if (AppConfig.RootCauseAnalysis.METHOD == "macrobase")
        {
          val summarizerSpec = EWStreamingSummarizerSpec(
            summaryUpdatePeriod = 20,
            decayType = "TUPLE_BASED",
            decayRate = 0.03,
            outlierItemSummarySize = 1000,
            inlierItemSummarySize = 1000,
            minOIRatio = 1,
            minSupport = 0.01,
            attributes = AppConfig.InputStream.DIMENSION_NAMES,
            attributeCombinations = true,
            summaryGenerationPeriod = 99
          )

          new EWStreamingSummarizer(summarizerSpec).runSearch(anomaliesStream).addSink(RCAResultJsonProducer())
        }
    }


    env.execute("Anomaly Detection Job")
  }
}

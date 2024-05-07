package root_cause_analysis

import aggregators.EWFeatureTransform
import anomaly_detection.detectors.{EWAppxPercentileAuxiliary, EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec}
import config.AppConfig
import models.{AggregatedRecordsWBaseline, AnomalyEvent, Dimension, RCAResult}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.junit.Test
import utils.Types

import java.io.FileReader
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EWStreamingSummarizerTest {
  def readCSV(csvPath: String): List[AggregatedRecordsWBaseline] = {
    val fileReader = new FileReader(csvPath)

    val csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT)

    val csvRecords = csvParser.getRecords

    val headers = csvRecords.get(0)
    val records = ListBuffer[AggregatedRecordsWBaseline]()

    for (i <- 1 until csvRecords.size()) {
      val csvRecord = csvRecords.get(i)
      val metric = csvRecord.get(4)
      val currentDimensionBreakdown = mutable.HashMap[Dimension, Types.MetricValue]()
      val dimensionHierarchy = mutable.HashMap[Types.ChildDimension, Types.ParentDimension]()
      for (a <- 0 until headers.size())
      {
        if (a != 4)
        {
          val attributeName = headers.get(a)
          val dimension = Dimension(attributeName, csvRecord.get(a), group=attributeName, level=1)
          currentDimensionBreakdown.put(dimension, metric.toDouble)
        }
      }
      val record = AggregatedRecordsWBaseline(metric.toDouble, metric.toDouble, currentDimensionBreakdown.toMap, currentDimensionBreakdown.toMap, dimensionHierarchy.toMap, 1)
      records += record
    }

    // Close the CSV parser and file reader
    csvParser.close()
    fileReader.close()
    records.toList
  }

  @Test
  def testMADAnalyzer(): Unit = {
    // Input Stream Spec
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)
    env.setParallelism(1)

    val csvPath = "src/test/resources/low_metric_simple.csv"
    val records: List[AggregatedRecordsWBaseline] = readCSV(csvPath)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 10
    anomalySpec.warmupCount = 10
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 50
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 50
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.80

    // Root Cause Analysis Spec
    val batchSize = 1000
    val attributes = List("A1", "A2", "A3", "A4", "A5")
    val summarizerSpec = new EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 50,
      decayType = "TUPLE_BASED",
      decayRate = 0.01,
      outlierItemSummarySize = 1000,
      inlierItemSummarySize = 1000,
      minOIRatio = 1,
      minSupport = 0.02,
      attributes = attributes,
      attributeCombinations = true,
      maximumSummaryDelay = batchSize
    )

    // MAD training Initialization
    val featureTransform = new EWFeatureTransform(anomalySpec)

    // Anomaly Detection Initialization
    val detector = new EWAppxPercentileAuxiliary(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec, batchSize)

    // Input Stream Initialization
    val dataStream: DataStream[AggregatedRecordsWBaseline] = env.fromCollection(records)

    val countStream: DataStream[Int] = dataStream.map(_ => 1)

//    // MAD training
//    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = dataStream
//      .flatMap(featureTransform)
//
//    // Anomaly Detection
//    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
//      .flatMap(detector)
//
//    // Root Cause Analysis
//    summarizer.consume(anomalyStream = anomalyEventStream)
//    val summaryStream: DataStream[RCAResult] = summarizer.runSearch(env)
//
//    // Output
//    summaryStream.print()
    env.execute("test MAD Analyzer")
  }
}

package root_cause_analysis

import aggregators.EWFeatureTransform
import anomaly_detection.detectors.{EWAppxPercentileAuxiliary, EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec}
import config.AppConfig
import models._
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.junit.Assert.assertEquals
import org.junit.Test
import sources.kafka.InputRecordStreamBuilder
import utils.Types

import java.io.FileReader
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class EWStreamingSummarizerTest {
  def readCSV(csvPath: String, metric_place: Int): List[AggregatedRecordsWBaseline] = {
    val fileReader = new FileReader(csvPath)

    val csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT)

    val csvRecords = csvParser.getRecords

    val headers = csvRecords.get(0)
    val records = ListBuffer[AggregatedRecordsWBaseline]()

    for (i <- 1 until csvRecords.size()) {
      val csvRecord = csvRecords.get(i)
      val metric = csvRecord.get(metric_place)
      val currentDimensionBreakdown = mutable.HashMap[Dimension, Types.MetricValue]()
      val dimensionHierarchy = mutable.HashMap[Types.ChildDimension, Types.ParentDimension]()
      for (a <- 0 until headers.size())
      {
        if (a != metric_place)
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
    val parallelism: Int = 1
    env.setParallelism(parallelism)
    val csvPath = "src/test/resources/low_metric_simple.csv"
    val records: List[AggregatedRecordsWBaseline] = readCSV(csvPath, 4)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 10
    anomalySpec.warmupCount = 10
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 50
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 50
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.99

    // Root Cause Analysis Spec
    val batchSize = 1000
    val attributes = List("A1", "A2", "A3", "A4")
    val summarizerSpec = new EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 50,
      decayType = "TUPLE_BASED",
      decayRate = 0.01,
      outlierItemSummarySize = 1000,
      inlierItemSummarySize = 1000,
      minOIRatio = 1,
      minSupport = 0.02,
      attributes = attributes,
      attributeCombinations = true
    )

    // MAD training Initialization
    val featureTransform = new EWFeatureTransform(anomalySpec)

    // Anomaly Detection Initialization
    val detector = new EWAppxPercentileAuxiliary(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec, batchSize)

    // Input Stream Initialization
    val dataStream: DataStream[AggregatedRecordsWBaseline] = env.fromCollection(records)

    // MAD training
    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = dataStream
      .flatMap(featureTransform)

    // Anomaly Detection
    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
      .flatMap(detector)

    // Root Cause Analysis
    val summaryStream: DataStream[RCAResult] = summarizer.runSearch(anomalyEventStream)

    // Output
    val summaries: List[RCAResult] = summaryStream.executeAndCollect().toList

    assertEquals(parallelism, summaries.size)
    assertEquals(1, summaries.head.dimensionSummaries.size)
    assertEquals("A1", summaries.head.dimensionSummaries.head.dimension.name)
    assertEquals("0", summaries.head.dimensionSummaries.head.dimension.value)
  }

  @Test
  def testSensor10KPower(): Unit = {
    // Input Stream Spec
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)
    val parallelism: Int = 1
    env.setParallelism(parallelism)

    val csvPath = "src/test/resources/sensor10k_filtered.csv"
    val records: List[AggregatedRecordsWBaseline] = readCSV(csvPath, 0)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 10
    anomalySpec.warmupCount = 10
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 1000
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 1000
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.99

    // Root Cause Analysis Spec
    val batchSize = 1000
    val attributes = List("device_id")
    val summarizerSpec = new EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 50,
      decayType = "TUPLE_BASED",
      decayRate = 0.01,
      outlierItemSummarySize = 1000,
      inlierItemSummarySize = 1000,
      minOIRatio = 1,
      minSupport = 0.01,
      attributes = attributes,
      attributeCombinations = true
    )

    // MAD training Initialization
    val featureTransform = new EWFeatureTransform(anomalySpec)

    // Anomaly Detection Initialization
    val detector = new EWAppxPercentileAuxiliary(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec, batchSize)

    // Input Stream Initialization
    val dataStream: DataStream[AggregatedRecordsWBaseline] = env.fromCollection(records)

    // MAD training
    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = dataStream
      .flatMap(featureTransform)

    // Anomaly Detection
    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
      .flatMap(detector)

    // Root Cause Analysis
    val summaryStream: DataStream[RCAResult] = summarizer.runSearch(anomalyEventStream)

    // Output
    val summaries: List[RCAResult] = summaryStream.executeAndCollect().toList

    assertEquals(1, summaries.size)
    assertEquals(1, summaries.head.dimensionSummaries.size)
    assertEquals("device_id", summaries.head.dimensionSummaries.head.dimension.name)
    assertEquals("2040", summaries.head.dimensionSummaries.head.dimension.value)
  }

  @Test
  def testSensor10KTemp(): Unit = {
    // Input Stream Spec
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)
    val parallelism: Int = 4
    env.setParallelism(parallelism)

    val csvPath = "src/test/resources/low_metric_sensor10k.csv"
    val records: List[AggregatedRecordsWBaseline] = readCSV(csvPath, 3)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 10
    anomalySpec.warmupCount = 100
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 1000
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 1000
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.82

    // Root Cause Analysis Spec
    val batchSize = 1000
    val attributes = List("device_id", "model", "firmware_version")
    val summarizerSpec = new EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 1000,
      decayType = "TUPLE_BASED",
      decayRate = 0.01,
      outlierItemSummarySize = 1000,
      inlierItemSummarySize = 1000,
      minOIRatio = 1,
      minSupport = 0.06,
      attributes = attributes,
      attributeCombinations = true
    )

    // MAD training Initialization
    val featureTransform = new EWFeatureTransform(anomalySpec)

    // Anomaly Detection Initialization
    val detector = new EWAppxPercentileAuxiliary(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec, batchSize)

    // Input Stream Initialization
    val dataStream: DataStream[AggregatedRecordsWBaseline] = env.fromCollection(records)

    // MAD training
    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = dataStream
      .flatMap(featureTransform)

    // Anomaly Detection
    val anomalyEventStream: DataStream[AnomalyEvent] = aggregatedRecordsWScore
      .flatMap(detector)


    // Root Cause Analysis
    val summaryStream: DataStream[RCAResult] = summarizer.runSearch(anomalyEventStream)

    // Output
    val summaries: List[RCAResult] = summaryStream.executeAndCollect().toList
    print(summaries)

//    assertEquals(3, summaries.size)
//    assertEquals(1, summaries.head.dimensionSummaries.size)
//    assertEquals("device_id", summaries.head.dimensionSummaries.head.dimension.name)
//    assertEquals("2040", summaries.head.dimensionSummaries.head.dimension.value)
  }

  @Test
  def testFromKafka(): Unit = {
    // Input Stream Spec
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)

    val parallelism: Int = 2
    env.setParallelism(parallelism)

    // Anomaly Detector Spec
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 100 // How big the sample should be
    anomalySpec.warmupCount = 50
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 50
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 50
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.8

    // Root Cause Analysis Spec
    val attributes = AppConfig.InputStream.DIMENSION_NAMES
    val summarizerSpec = EWStreamingSummarizerSpec(
      summaryUpdatePeriod = 100,
      decayType = "TUPLE_BASED",
      decayRate = 0.01,
      outlierItemSummarySize = 1000,
      inlierItemSummarySize = 1000,
      minOIRatio = 1,
      minSupport = 0.01,
      attributes = attributes,
      attributeCombinations = true
    )

    // Anomaly Detection
    val anomalyDetector = new EWAppxPercentileOutlierClassifier
    anomalyDetector.init(anomalySpec)

    // Root Cause Analysis Initialization
    val summarizer = new EWStreamingSummarizer(summarizerSpec, summarizerSpec.summaryUpdatePeriod)

    // Input Stream Initialization
    val dataStream: DataStream[InputRecord] = InputRecordStreamBuilder
      .buildInputRecordStream(env)

    val anomalyEventStream: DataStream[AnomalyEvent] = anomalyDetector.runDetection(dataStream)

    // Root Cause Analysis
    val summaryStream: DataStream[RCAResult] = summarizer.runSearch(anomalyEventStream)

    summaryStream.print()

    env.execute("Test from Kafka")
  }

}

package root_cause_analysis

import aggregators.EWFeatureTransform
import anomaly_detection.detectors.{EWAppxPercentileAuxiliary, EWAppxPercentileOutlierClassifier, EWAppxPercentileOutlierClassifierSpec}
import config.AppConfig
import models.{AggregatedRecordsWBaseline, Dimension, InputRecord}
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.junit.Test
import utils.Types

import java.io.FileReader
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

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

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    AppConfig.enableCheckpoints(env)

    val csvPath = "src/test/resources/simple.csv"
    val records: List[AggregatedRecordsWBaseline] = readCSV(csvPath)

    val dataStream: DataStream[AggregatedRecordsWBaseline] = env.fromCollection(records)

    // Anomaly Detector
    val anomalySpec = new EWAppxPercentileOutlierClassifierSpec()
    anomalySpec.sampleSize = 10
    anomalySpec.warmupCount = 10
    anomalySpec.trainingPeriodType = "TUPLE_BASED"
    anomalySpec.trainingPeriod = 50
    anomalySpec.decayPeriodType = "TUPLE_BASED"
    anomalySpec.decayPeriod = 50
    anomalySpec.decayRate = 0.01
    anomalySpec.percentile = 0.80

    // MAD training
    val featureTransform = new EWFeatureTransform(anomalySpec)

    val aggregatedRecordsWScore: DataStream[(AggregatedRecordsWBaseline, Double)] = dataStream
      .flatMap(featureTransform)

    // Anomaly Detection
    val detector = new EWAppxPercentileAuxiliary(anomalySpec)

    val anomalyEventStream: DataStream[(AggregatedRecordsWBaseline, Boolean)] = aggregatedRecordsWScore
      .flatMap(detector)

    anomalyEventStream.print()

    env.execute("test MAD Analyzer")
  }
}

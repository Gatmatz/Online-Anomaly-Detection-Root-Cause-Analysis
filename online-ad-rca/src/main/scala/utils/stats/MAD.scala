package utils.stats

import models.{AggregatedRecordsWBaseline, InputRecord}

import scala.util.Sorting
class MAD {
  private var median: Double = _
  private var MAD: Double = _

  private val trimmedMeanFallback: Double = 0.05
  // https://en.wikipedia.org/wiki/Median_absolute_deviation#Relation_to_standard_deviation
  private val MAD_TO_ZSCORE_COEFFICIENT: Double = 1.4826
  def train(data: List[AggregatedRecordsWBaseline]): Unit =
  {
    val len: Int = data.size
    val metrics: Array[Double] = new Array[Double](data.size)
    var i: Int = 0
    while (i < len) {
      metrics(i) = data(i).current
      i += 1
    }

    Sorting.quickSort(metrics)

    median =
      {
        if (len % 2 == 0) {
          (metrics(len / 2 - 1) + metrics(len / 2)) / 2
        } else {
          metrics((len / 2).toDouble.ceil.toInt)
        }
      }

    val residuals = new Array[Double](len)
    i = 0
    while (i < len)
    {
      residuals(i) = math.abs(metrics(i) - median)
      i += 1
    }

    Sorting.quickSort(residuals)

    MAD =
    {
      if (data.size % 2 == 0) {
        (residuals(data.size / 2 - 1) + residuals(data.size / 2)) / 2
      } else {
        residuals(data.size / 2)
      }
    }

    if (MAD == 0) {
      val lowerTrimmedMeanIndex: Int = (residuals.length * trimmedMeanFallback).toInt
      val upperTrimmedMeanIndex: Int = (residuals.length * (1 - trimmedMeanFallback)).toInt
      var sum: Double = 0
      i = lowerTrimmedMeanIndex
      while (i < upperTrimmedMeanIndex) {
        sum += residuals(i)
        i += 1
      }
      MAD = sum / (upperTrimmedMeanIndex - lowerTrimmedMeanIndex)
    }
  }

  def score(record: AggregatedRecordsWBaseline): Double = {
    val point: Double = record.current
    math.abs(point - median) / MAD
  }

  def getZScoreEquivalent(zscore: Double): Double =
  {
    zscore / MAD_TO_ZSCORE_COEFFICIENT
  }
}

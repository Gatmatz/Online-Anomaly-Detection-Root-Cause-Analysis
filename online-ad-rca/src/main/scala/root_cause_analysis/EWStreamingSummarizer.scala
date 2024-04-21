package root_cause_analysis
import models.{AnomalyEvent, RCAResult}
import org.apache.flink.streaming.api.scala.DataStream

class EWStreamingSummarizer extends ContributorsFinder {

  override def runSearch(anomalyStream: DataStream[AnomalyEvent]): DataStream[RCAResult] = {

  }
}

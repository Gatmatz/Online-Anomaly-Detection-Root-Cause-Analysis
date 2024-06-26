package models

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode

import java.time.LocalDateTime
  case class RCAResult(
                      relatedAnomalyId: String,
                      detectedAt: LocalDateTime,
                      currentTotal: Double,
                      baselineTotal: Double,
                      support: Double,
                      numRecords: Double,
                      ratioToInliers: Double,
                      dimensionGroup: String,
                      dimensionSummaries: List[DimensionSummary]
                    ) {
  override def toString: String = {
    "RCAResult(relatedAnomalyId=%s, detectedAt=%s, currentTotal=%s, baselineTotal=%s, support=%s, numRecords=%s, ratioToInliers=%s, dimensionGroup=%s, dimensionSummaries=%s)".format(
      relatedAnomalyId,
      detectedAt,
      currentTotal,
      baselineTotal,
      support,
      numRecords,
      ratioToInliers,
      dimensionGroup,
      dimensionSummaries.mkString(", ")
    )
  }

  def toObjectNode(objectMapper: ObjectMapper): ObjectNode = {
    val node: ObjectNode = objectMapper.createObjectNode()
    node.put("relatedAnomalyId", relatedAnomalyId)
    node.put("detectedAt", detectedAt.toString)
    node.put("currentTotal", currentTotal)
    node.put("baselineTotal", baselineTotal)
    node.put("support", support)
    node.put("numRecords", numRecords)
    node.put("ratioToInliers", ratioToInliers)
    node.put("dimensionGroup", dimensionGroup)

    // create an array for dimensionSummaries
    val dimensionSummariesArray = objectMapper.createArrayNode()

    // parse each item in dimensionsSummaries list to ObjectNode and add it to dimensionSummariesArray
    dimensionSummaries
      .foreach(dimensionSummary => dimensionSummariesArray.add(dimensionSummary.toObjectNode(objectMapper)))
    node.set("dimensionSummaries", dimensionSummariesArray)

    node
  }
}
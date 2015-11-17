package com.elteuplat


import io.prediction.controller.EngineFactory
import io.prediction.controller.Engine

case class Query(items: Array[String], num: Int, blackList: Array[String]) extends Serializable

case class PredictedResult(itemScores: Array[ItemScore]) extends Serializable

case class ItemScore(item: Item, score: Double) extends Serializable with Ordered[ItemScore] {
  def compare(that: ItemScore) = this.score.compare(that.score)
}

object SVDItemSimilarityEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("algo" -> classOf[Algorithm]),
      classOf[Serving])
  }
}
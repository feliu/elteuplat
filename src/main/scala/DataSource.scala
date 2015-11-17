package com.elteuplat

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.PropertyMap
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData, EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // creem un RDD format per (entityID, Item)
    val itemsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId:String, properties:PropertyMap) =>
      val item = try {
        //llegim el json i creem un Item
        val ingredients: Array[String] = properties.get[Array[String]]("ingredients")
        val name: String = properties.get[String]("name")
        val bitter: Int = properties.get[Int]("bitter")
        val meaty: Int = properties.get[Int]("meaty")
        val piquant: Int = properties.get[Int]("piquant")
        val salty: Int = properties.get[Int]("salty")
        val sour: Int = properties.get[Int]("sour")
        val sweet: Int = properties.get[Int]("sweet")
        val image : String = properties.get[String]("image")
        Item(entityId,name, ingredients,bitter, meaty,piquant,salty,sour,sweet,image)

      }
      catch
        {
        case e: Exception => {
          logger.error(s"No hem pogut agafar les propietats ${properties} de l'element  ${entityId}. Excepcio: ${e}.")
          throw e
        }
      }
      (entityId, item)
    }

    new TrainingData(items = itemsRDD)
  }
}

case class Item(id: String,name:String, ingredients: Array[String],
                bitter: Int,meaty: Int,piquant: Int,salty: Int,
                sour: Int,sweet: Int, image:String)

class TrainingData(val items: RDD[(String, Item)]) extends Serializable {
  override def toString = {

    s"items: [${items.count()}] (${items.take(2).toList}...)"
  }
}
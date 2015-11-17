package com.elteuplat

import io.prediction.controller.{P2LAlgorithm, Params}
import io.prediction.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.feature.{Normalizer, StandardScaler}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import scala.reflect.ClassTag

class Model(val itemIds: BiMap[String, Int], val projection: DenseMatrix,val preparedData: scala.collection.Map[String,Item])
  extends Serializable {
  override def toString = s"Items: ${itemIds.size}"
}

case class AlgorithmParams(dimensions: Int) extends Params


class Algorithm(val ap: AlgorithmParams) extends P2LAlgorithm[PreparedData, Model, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  //codifica un array d'strings, utilitzant el one hot encoder
  private def encode(data: RDD[Array[String]]): RDD[Vector] = {
    val dict = BiMap.stringLong(data.flatMap(x => x))
    val len = dict.size

    val vector = data.map { sample:Array[String] =>
      val indexes = sample.map(dict(_).toInt).sorted
      //utilitzem els SparseVector per no haver de guardar totes els zeros que genera el one hot encoder.
      Vectors.sparse(len, indexes, Array.fill[Double](indexes.length)(1.0))
    }
    vector

  }
  //codifica un String utilitzant el one hot encoder
  private def encode[X: ClassTag](data: RDD[String]): RDD[Vector] = {
    val dict = BiMap.stringLong(data)
    val len = dict.size

    data.map { sample =>
      val index = dict(sample).toInt
      //utilitzem els SparseVector per no haver de guardar totes els zeros que genera el one hot encoder.
      Vectors.sparse(len, Array(index), Array(1.0))
    }
  }

  //unifica dos vectors, ja siguin SparseVector, és a dir resultants de la codificació del one hot encoder, o siguin DenseVector de les variables numèriques.
  private def merge(v1: RDD[Vector], v2: RDD[Vector]): RDD[Vector] = {
    val merge = v1.zip(v2) map {
      case (SparseVector(leftSz, leftInd, leftVals), SparseVector(rightSz, rightInd, rightVals)) =>
        Vectors.sparse(leftSz + rightSz, leftInd ++ rightInd.map(_ + leftSz), leftVals ++ rightVals)
      case (SparseVector(leftSz, leftInd, leftVals), DenseVector(rightVals)) =>
        Vectors.sparse(leftSz + rightVals.length, leftInd ++ rightVals.indices.map(_ + leftSz), leftVals ++ rightVals)
    }

    merge // resultat de la unificació
  }

  private def transposeRDD(data: RDD[Vector]) = {
    val len = data.count().toInt

    val byColumnAndRow = data.zipWithIndex().flatMap {
      case (rowVector, rowIndex) => { rowVector match {
        case SparseVector(_, columnIndices, values) =>
          values.zip(columnIndices)
        case DenseVector(values) =>
          values.zipWithIndex
      }} map {
        case(v, columnIndex) => columnIndex -> (rowIndex, v)
      }
    }

    val byColumn = byColumnAndRow.groupByKey().sortByKey().values

    val transposed = byColumn.map {
      indexedRow =>
        val all = indexedRow.toArray.sortBy(_._1)
        val significant = all.filter(_._2 != 0)
        Vectors.sparse(len, significant.map(_._1.toInt), significant.map(_._2))
    }

    transposed
  }


  /**
    *
    * @param sc contex de l'Spark
    * @param data tots els plats ja preparats per la generació del model
    * @return el model de les nostres dades, és a dir la matriu sobre el qual podrem extreure les similituds.
    */
  def train(sc: SparkContext, data: PreparedData): Model = {
    val itemIds : BiMap[String,Int] = BiMap.stringInt(data.items.map(x => x._1))

    //Per codificar les variables categoriques utilitzem el one-hot encoder
    val categorical = Seq(encode(data.items.map(_._2.ingredients)),encode(data.items.map(_._2.name)))


    /**
      * Transformem les variables numèriques: les variables numèriques son escalades i
      * he deixat uns pesos per si algun dia és vol canviar la prioritat d'una variables sobre una altre
      */
    val numericRow : RDD[Vector] = data.items.map(x =>
      Vectors.dense(x._2.bitter,x._2.meaty,x._2.piquant,x._2.salty,x._2.sour,x._2.sweet))
    val weights = Array(1,1,1,1,1,1)
    val scaler = new StandardScaler(withMean = true,
      withStd = true).fit(numericRow)

    //Estandarització https://en.wikipedia.org/wiki/Feature_scaling + multiplicació per uns pesos que a priori són 1
    val numeric = numericRow.map (
      x => Vectors.dense(scaler.transform(x).toArray.zip(weights).map {
        case (x, w) => x * w })).cache()


    //En aquest punt normalitzem els vectors, d'aquesta forma obtindran la norma unitaria i el seu producte escalar produirà el cosinus entre vectors.
    val normalizer = new Normalizer(p = 2.0)
    val allData = normalizer.transform((categorical ++ Seq(numeric)).reduce(merge))


    //transposem la matriu ja que el mètode SVD treballa més bé, amb matrius que tinguin més files que columnes
    val transposed = transposeRDD(allData)

    val mat: RowMatrix = new RowMatrix(transposed)

    // Factoritzem la matriu per reduir la dimensionalitat de les dades
    val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(ap.dimensions, computeU = false)

    val V: DenseMatrix = new DenseMatrix(svd.V.numRows, svd.V.numCols,
      svd.V.toArray)

    //la matriu de projecció final, tindrà el tamany de número de plats x número de dimensions utilitzades (ap.dimensions)
    val projection = Matrices.diag(svd.s).multiply(V.transpose)

    new Model(itemIds, projection,data.items.collectAsMap()) //resultat de l'entrenament de les dades.
  }

  /**
    *
    * @param model que representa la similitud entre tots els plats
    * @param query consulta que fem sobre el model i que retorna una predicció
    * @return La predicció
    */
  def predict(model: Model, query: Query): PredictedResult = {

    //Com que no tenim elements sobre els quals fer una predició, retornem una mostra aleatori, útil per iniciar l'aplicació
    if(query.items.isEmpty) {
      val r = scala.util.Random
      val max : Int = model.itemIds.size
      PredictedResult((1 to query.num).map { i =>
        val random = Math.abs(r.nextInt())
        ItemScore(model.preparedData.values.toArray.apply(Math.abs(random % model.preparedData.size)),1)
      }.toArray)
    }
    else // En cas que ja tinguem elements per fer la predicció
    {

      val result = query.items.flatMap { itemId =>
        model.itemIds.get(itemId).map { j =>

          //columna amb totes les valoracions del plat actual (itemId)
          val d = for(i <- 0 until model.projection.numRows) yield model.projection(i, j)
          //valoracions de tots els plats en relació al plat actual (itemId)
          val col = model.projection.transpose.multiply(new DenseVector(d.toArray))
          //Generem un ItemScore amb totes les dades del plat i la puntuació respectiva
          for(k <- 0 until col.size) yield new ItemScore(model.preparedData.get(model.itemIds.inverse.getOrElse(k, default="NA")).get, col(k))
        }.getOrElse(Seq())
      }.groupBy {
        case(ItemScore(itemId, _)) => itemId //agrupem tots els resultats
      }.map(_._2.max).filter {
        case(ItemScore(itemId, _)) => !query.items.contains(itemId.id) // filtrem els plats de la query del resultat.
      }.toArray.sorted.reverse // Ordenem els resultats

      //Responem amb el resultat de la predicció, en aquest punt fem una úlitma cosa. Si ens han passat plats que no poden sortir en la predicció, els filtrem.
      //Exemple: els plats que s'estàn mostrant a l'app, però encara no tenen un veredicte.
      //Després agafem el número de resultats esperats per la query inicial.
      PredictedResult(result.filter(itemScore => !query.blackList.contains(itemScore.item.id)).take(query.num))


    }

  }
}
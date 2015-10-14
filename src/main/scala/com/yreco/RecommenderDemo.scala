package com.yreco

import java.io.{File, PrintWriter}

import com.yreco.YrecoIntSerializer._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{RowMatrix, CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try
/**
 * Created by yawo on 20/09/15.
 *
 *  Submit with:
 *  $SPARK_HOME/bin/spark-submit --class "com.yreco.RecommenderDemo" --master local[4] /home/yawo/Studio/ytech/others/finagle/target/scala-2.11/yreco-assembly-1.0.jar
 */
object RecommenderDemo{

    //CONFIG
    @transient val hbaseConf     = HBaseConfiguration.create()
    @transient val conf          = new SparkConf().setAppName("Erecommender")
    @transient lazy val scProxy  = SparkContext.getOrCreate(conf) //new SparkContext(conf);
    val viewTable                = "view"
    val ProductColumnFamily      = Bytes.toBytes("p")
    val ProductIdColumnQualifier = Bytes.toBytes("id")
    val UserColumnFamily         = Bytes.toBytes("u")
    val UserIdColumnQualifier    = Bytes.toBytes("id")
    val rank                     = 100
    val numIterations            = 20
    val similarityFile           = new File("/tmp/sims.csv")
    val ratingsSimilarityFile    = new File("/tmp/sims_ratings.csv")
    val recoByUserFile           = new File("/tmp/reco.csv")

    hbaseConf.set("hbase.master", "localhost:60000")
    hbaseConf.setInt("timeout", 120000)
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, viewTable)

    //FETCH DATA FROM HBASE

    var ratings: RDD[Rating] = null;
    def loadRatings = {
      val viewRDD: RDD[(ImmutableBytesWritable, Result)] = scProxy.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      ratings = viewRDD.map {
        case (key, res) => Try(Bytes.toString(res.getValue(ProductColumnFamily, ProductIdColumnQualifier)).toInt).toOption.map {
          new Rating(Bytes.toString(res.getValue(UserColumnFamily, UserIdColumnQualifier)).toInt, _, 1D)
        }.orNull
      }.filter(_ != null).distinct()
    }

    //BUILD THE RECO MODEL USING ALS ALGORITHM
    var model: MatrixFactorizationModel = null
    var usersByProductCount: IndexedRDD[Int, Int] = null
    def trainModel = {
      model               = ALS.trainImplicit(ratings, rank, numIterations)
      val usersByProduct  = ratings.map {case Rating(user, product, rate) => (user, product) }
      usersByProductCount = IndexedRDD(usersByProduct.combineByKey[Int]((v: Int) => 1, (c: Int, v:Int) => c + 1, (c1: Int, c2: Int) => c1 + c2)).cache()
      computeSimilarityFromRatings(usersByProduct)
    }


    def computeSimilarityFromRatings(usersByProduct:RDD[(Int,Int)],nMaxSims:Int = 20) = {
      val sortedProductsArr = usersByProduct.map{case (usr,pdt) => (pdt,1)}.distinct().sortByKey().keys.collect()
      val sortedProductsMap = sortedProductsArr.zipWithIndex.toMap[Int,Int]
      val productsSize      = sortedProductsArr.size
      val rowMatrix         = new RowMatrix(usersByProduct.groupByKey().map { case (user, productsArray) =>
          Vectors.sparse(productsSize,productsArray.map(sortedProductsMap).zip(Seq.fill(productsArray.size)(1.0)).toSeq)
      })
      val simMatrix = rowMatrix.columnSimilarities()
      val topSimsFromRatings:Array[(Int,Int)] = simMatrix.entries.top(nMaxSims)(Ordering.by(_.value)).map {
        case MatrixEntry(productIndex1,productIndex2,simLevel) => (sortedProductsArr(productIndex1.toInt),sortedProductsArr(productIndex2.toInt))
      }
      writeSimilarities(topSimsFromRatings,ratingsSimilarityFile)
    }

    def computeSimilarityFromFeatures(nMaxSims:Int = 20) = {
      //Similarities
      val pFtIdx    = model.productFeatures.zipWithIndex
      val entries   = pFtIdx.flatMap { t => for (i <- t._1._2.indices) yield MatrixEntry(i, t._2, t._1._2(i)) }
      val dic       = pFtIdx.map { t => (t._2, t._1._1) }.collectAsMap()
      val simMatrix = new CoordinateMatrix(entries).toRowMatrix().columnSimilarities()
      val topSims = simMatrix.entries.top(nMaxSims)(Ordering.by(_.value)).map {
        case MatrixEntry(productIndex1,productIndex2,simLevel) => (dic(productIndex1),dic(productIndex2))
      }
      writeSimilarities(topSims,similarityFile)
    }
    def computeSimilarity(nMaxSims:Int = 20) = {computeSimilarityFromFeatures(nMaxSims)}

    def writeSimilarities(sims:Array[(Int,Int)], file:File): Unit ={
      val writer  = new PrintWriter(file)
      for( (product1,product2) <- sims){
        writer.write(product1.toString)
        writer.write(",")
        writer.write(product2.toString)
        writer.write("\n")
      }
      writer.close
    }

    def computeRecos(numberOfRecommendation: Int = 3) = {
      val writer = new PrintWriter(recoByUserFile)
      for ((userId, productCount) <- usersByProductCount.toLocalIterator) {
        writer.write(userId.toString)
        writer.write(",")
        writer.write(model.recommendProducts(userId, productCount + numberOfRecommendation).map(_.product).drop(productCount).take(numberOfRecommendation).mkString(","))
        writer.write("\n")
      }
      writer.close
    }
}

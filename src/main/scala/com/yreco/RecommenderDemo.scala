package com.yreco

import java.io.{File, PrintWriter}

import com.yreco.YrecoIntSerializer._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
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
    @transient val hbaseConf = HBaseConfiguration.create()
    @transient val conf = new SparkConf().setAppName("Erecommender")
    @transient lazy val scProxy = SparkContext.getOrCreate(conf) //new SparkContext(conf);
    val viewTable = "view"
    val ProductColumnFamily = Bytes.toBytes("p")
    val ProductIdColumnQualifier = Bytes.toBytes("id")
    val UserColumnFamily = Bytes.toBytes("u")
    val UserIdColumnQualifier = Bytes.toBytes("id")
    val rank = 10
    val numIterations = 20
    val similarityFile = "/tmp/sims.csv"
    val recoByUserFile = "/tmp/reco.csv"

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
      }.filter(_ != null)
    }

    //BUILD THE RECO MODEL USING ALS ALGORITHM
    var model: MatrixFactorizationModel = null
    var usersByProductCount: IndexedRDD[Int, Int] = null
    def trainModel = {
      model = ALS.trainImplicit(ratings, rank, numIterations)
      usersByProductCount = IndexedRDD(ratings.map {
        case Rating(user, product, rate) => (user, 1)
      }.combineByKey[Int]((u: Int) => 1, (c: Int, u: Int) => c + 1, (c1: Int, c2: Int) => c1 + c2)).cache()
    }


    def computeSimilarity(nMaxSims:Int = 50) = {
      //Similarities
      val pfi = model.productFeatures.zipWithIndex
      val entries = pfi.flatMap { t => for (i <- t._1._2.indices) yield MatrixEntry(i, t._2, t._1._2(i)) }
      val dic = pfi.map { t => (t._2, t._1._1) }.collectAsMap()
      val topSims = new CoordinateMatrix(entries).toRowMatrix().columnSimilarities(0.01).entries.map {
        case MatrixEntry(i, j, u) => ((i, j), u)
      }.sortBy((_._2), false).take(nMaxSims)
      val writer = new PrintWriter(new File(similarityFile))
      for( ((productIndex1,productIndex2),simLevel) <- topSims){
        writer.write(dic(productIndex1).toString)
        writer.write(",")
        writer.write(dic(productIndex2).toString)
        writer.write("\n")
      }
      writer.close
    }

    def computeRecos(numberOfRecommendation: Int = 3) = {
      val writer = new PrintWriter(new File(recoByUserFile))
      for ((userId, productCount) <- usersByProductCount.toLocalIterator) {
        writer.write(userId.toString)
        writer.write(",")
        writer.write(model.recommendProducts(userId, productCount + numberOfRecommendation).map(_.product).drop(productCount).take(numberOfRecommendation).mkString(","))
        writer.write("\n")
      }
      writer.close
    }
}

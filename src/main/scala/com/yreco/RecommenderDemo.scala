package com.yreco

import com.yreco.YrecoIntSerializer._
import com.twitter.finagle.{ListeningServer, ThriftMux}
import com.twitter.util.{Await, Duration, Future}
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import thrift.RecommenderEngine.FutureIface
import scala.util.Try

/**
 * Created by yawo on 20/09/15.
 * > $SPARK_HOME/bin/spark-shell --packages amplab:spark-indexedrdd:0.3
 */
object RecommenderDemo extends App {
  //CONFIG
  @transient val hbaseConf      = HBaseConfiguration.create()
  val conf                      = new SparkConf().setAppName("Erecommender")
  val sc                        = new SparkContext(conf)
  val viewTable                 = "view"
  val nUsersToRecommend         = 2
  val nProductsToRecommend      = 2
  val ProductColumnFamily       = Bytes.toBytes("p")
  val ProductIdColumnQualifier  = Bytes.toBytes("id")
  val UserColumnFamily          = Bytes.toBytes("u")
  val UserIdColumnQualifier     = Bytes.toBytes("id")
  val rank                      = 10
  val numIterations             = 20

  hbaseConf.set(    "hbase.master"              , "localhost:60000")
  hbaseConf.setInt( "timeout"                   , 120000)
  hbaseConf.set(    "hbase.zookeeper.quorum"    , "localhost")
  hbaseConf.set(    "zookeeper.znode.parent"    , "/hbase")
  hbaseConf.set(    TableInputFormat.INPUT_TABLE, viewTable)

  def fetchAndBuild(): Unit ={

  }
  //FETCH DATA FROM HBASE
  val viewRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
  val ratings: RDD[Rating] = viewRDD.map {
    case (key, res) => Try(Bytes.toString(res.getValue(ProductColumnFamily, ProductIdColumnQualifier)).toInt).toOption.map {
      new Rating(Bytes.toString(res.getValue(UserColumnFamily, UserIdColumnQualifier)).toInt, _, 1D)
    }.orNull
  }.filter(_ != null)

  //BUILD THE RECO MODEL USING ALS ALGORITHM
  val model:MatrixFactorizationModel = ALS.trainImplicit(ratings, rank, numIterations)

  val usersByProductCount = IndexedRDD(
          ratings.map {
            case Rating(user, product, rate) =>   (user, 1)
          }.combineByKey[Int]( (u)=>1 , (c,u) => c+1 , (c1,c2) => c1+c2 )
  ).cache()

  def recommendationProxy(userId: Int, numberOfRecommendation: Int, currentItemIds: collection.Set[Int]):Future[collection.Set[Int]] = {
    Future {
      println("Got userId %s".format(userId))
      val currentItemIdSet     = if(currentItemIds == null) Set.empty else currentItemIds
      val previousItemsIdCount = usersByProductCount.get(userId).getOrElse(0)
      val nToRecommend         = numberOfRecommendation + currentItemIdSet.size + previousItemsIdCount
      val recommended          = model.recommendProducts(userId,nToRecommend).map(_.product).drop(previousItemsIdCount).toSet
      recommended -- currentItemIdSet
    }
  }

  //SEND OUT TO SOLR
  //TODO

  //SERVE WITH FINAGLE THRIFTMUX SERVER.
  println("starting server...")
  val server: ListeningServer = ThriftMux.serveIface(":*",new FutureIface {
    override def getRecommendations(userId: Int, numberOfRecommendation: Int, currentItemIds: collection.Set[Int]): Future[collection.Set[Int]] = {
      recommendationProxy(userId,numberOfRecommendation,currentItemIds)
    }
  })
  Await.ready(server, Duration.fromSeconds(600))
}

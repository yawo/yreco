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
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import thrift.RecommenderEngine.FutureIface
import scala.util.Try
import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext._
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

  def merge(srcPath: String, dstPath: String): Unit =  {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), false, hadoopConfig, null)
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
          }.combineByKey[Int]( (u:Int)=>1 , (c:Int,u:Int) => c+1 , (c1:Int,c2:Int) => c1+c2 )
  ).cache()

  //Similarities
  val file = "/tmp/partitionned_sims.csv"
  FileUtil.fullyDelete(new File(file))
  val destinationFile= "/tmp/sims.csv"
  FileUtil.fullyDelete(new File(destinationFile))

  val nSims = 3
  val pfi = model.productFeatures.zipWithIndex
  val entries = pfi.flatMap { t => for (i <- t._1._2.indices) yield MatrixEntry(i, t._2, t._1._2(i)) }
  val dic = pfi.map { t => (t._2, t._1._1) }.collectAsMap()
  val sims = new CoordinateMatrix(entries).toRowMatrix().columnSimilarities(0.01).entries.map {
    case MatrixEntry(i, j, u) => ((i, j), u)
  }.sortBy((_._2), false).map{
    t => (dic(t._1._1),dic(t._1._2))
  }

  //SEND OUT TO SOLR
  sims.saveAsTextFile(file)
  merge(file, destinationFile)

  //TODO

  //SERVE WITH FINAGLE THRIFTMUX SERVER.
  def recommendationProxy(userId: Int, numberOfRecommendation: Int, currentItemIds: Seq[Int]):Future[Seq[Int]] = {
    Future {
      println("Got userId %s".format(userId))
      val currentItemIdSeq     = if(currentItemIds == null) Seq.empty else currentItemIds
      val previousItemsIdCount = usersByProductCount.get(userId).getOrElse(0)
      val nToRecommend         = numberOfRecommendation + currentItemIdSeq.size + previousItemsIdCount
      val recommended          = model.recommendProducts(userId,nToRecommend).map(_.product).drop(previousItemsIdCount)
      recommended.diff(currentItemIdSeq).take(numberOfRecommendation)
    }
  }

  println("starting server...")
  val server: ListeningServer = ThriftMux.serveIface(":*",new FutureIface {
    override def getRecommendations(userId: Int, numberOfRecommendation: Int, currentItemIds: Seq[Int]): Future[Seq[Int]] = {
      recommendationProxy(userId,numberOfRecommendation,currentItemIds)
    }
  })
  Await.ready(server, Duration.fromSeconds(600))
}

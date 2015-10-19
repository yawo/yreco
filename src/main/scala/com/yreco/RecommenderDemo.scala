package com.yreco

import java.io.{File, PrintWriter}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry, RowMatrix}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.{LinkedHashSet, Map => MutableMap}
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
    @transient lazy val sqlProxy = SQLContext.getOrCreate(scProxy)

    val viewTable                = "view"
    val ProductColumnFamily      = Bytes.toBytes("p")
    val ProductIdColumnQualifier = Bytes.toBytes("id")
    val UserColumnFamily         = Bytes.toBytes("u")
    val UserIdColumnQualifier    = Bytes.toBytes("id")
    val rank                     = 30
    val numIterations            = 20
    val similarityFile           = new File("/tmp/sims.csv")
    val ratingsSimilarityFile    = new File("/tmp/sims_ratings.csv")
    val recoByUserFile           = new File("/tmp/reco.csv")
    val countApproxAccuracy      = 0.001

    hbaseConf.set("hbase.master", "localhost:60000")
    hbaseConf.setInt("timeout", 120000)
    hbaseConf.set("hbase.zookeeper.quorum", "localhost")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, viewTable)

    import sqlProxy.implicits._
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

    def loadIncrementalRatings(filePath: String) = {
      val viewRDD = scProxy.textFile(filePath).map { case str: String => Try {
          val arr = str.split(',');
          new Rating(arr(1).toInt, arr(2).toInt, 1D)
        }.getOrElse(null)
      }.filter(_ != null).distinct()

      ratings = viewRDD ++ ratings
    }



    //BUILD THE RECO MODEL USING ALS ALGORITHM
    var model: MatrixFactorizationModel             = null
    //var usersByProductCount: IndexedRDD[Int, Int]  = null
    var usersByProductCount: DataFrame  = null
    var usersByProduct:RDD[(Int,Int)]               = null
    // var productByUserCount:RDD[(Int,Int)]         = null
    var productByUser:RDD[(Int,Int)]                = null
    // var sortedProductsArr:Array[Int]              = null

    def countByKey[K,V](rdd:RDD[(K,V)])(implicit f:(RDD[(K,V)] => PairRDDFunctions[K,V])): RDD[(K,Int)] ={ rdd.aggregateByKey[Int](1)((c: Int, v:V) => c+1, (c1: Int, c2: Int) => c1+c2)  } //or .countApproxDistinctByKey(countApproxAccuracy)

    def writeSimilarities[B](sims:Seq[(Int,B)], file:File): Unit ={
      val writer  = new PrintWriter(file)
      for( (product1,product2) <- sims){
        writer.write(product1.toString)
        writer.write(",")
        product2 match {
          case p:Int      => writer.write(product2.toString)
          case Seq(s@_*)  => writer.write(s.mkString(","))
          case Array(s@_*)  => writer.write(s.mkString(","))
          case _          => println(s"matching problem for ${product2}")
        }

        writer.write("\n")
      }
      writer.close
    }

    def computeSimilarityFromRatings(nMaxSims:Int = 20) = {
      val productByUserCount  = countByKey(productByUser).sortByKey().filter(_._2>1) //consider product with multiple users.
      val sortedProductsArr   = productByUserCount.keys.collect()
      val sortedProductsMap   = sortedProductsArr.zipWithIndex.toMap[Int,Int]
      val productsSize        = sortedProductsArr.size
      val rowMatrix           = new RowMatrix(usersByProduct.groupByKey().map { case (user, productsArray) =>
          Vectors.sparse(productsSize,productsArray.filter(sortedProductsMap.contains).map(sortedProductsMap).zip(Seq.fill(productsArray.size)(1.0)).toSeq)
      })
      val simMatrix = rowMatrix.columnSimilarities(0.05)
      val topSimsFromRatings:Array[(Int,Int)] = simMatrix.entries.top(nMaxSims)(Ordering.by(_.value)).map {
        case MatrixEntry(productIndex1,productIndex2,simLevel) => (sortedProductsArr(productIndex1.toInt),sortedProductsArr(productIndex2.toInt))
      }
      val simsMap = MutableMap[Int,LinkedHashSet[Int]]().withDefault(_ => LinkedHashSet[Int]())
      topSimsFromRatings.foreach{case (p1,p2) => simsMap += (p1 -> (simsMap(p1) += p2),p2 -> (simsMap(p2) += p1))}
      writeSimilarities(simsMap.toSeq,ratingsSimilarityFile)
    }

    def trainModel = {
        model               = ALS.trainImplicit(ratings, rank, numIterations)
        usersByProduct      = ratings.map {case Rating(user, product, rate) => (user, product) }.cache()
        productByUser       = usersByProduct.map{case (usr,pdt) => (pdt,usr)}
        usersByProductCount = countByKey(usersByProduct).toDF("usr","pdtCount").cache()
        //usersByProductCount = IndexedRDD(countByKey(usersByProduct)).cache()
        computeSimilarityFromRatings()
        computeSimilarityFromCoocurrence()
    }



    def computeSimilarityFromCoocurrence(nMaxSims:Int = 20,nMaxSimsByProd:Int = 3,cooccurenceThreshold:Int = 4): Unit ={
      val coocc = countByKey(usersByProduct.join(usersByProduct).collect[((Int,Int),Int)] { case (user, (prod1, prod2)) if prod1 < prod2 => ((prod1, prod2),1)}
        ).reduceByKey{ (a: Int, b: Int) => a + b }.filter(_._2 > cooccurenceThreshold)

      val topCooccByProd = coocc.flatMap{ case (prod, count) =>
        Seq((prod._1, (prod._2, count)), (prod._2, (prod._1, count)))
      }.groupByKey.map { case (prod, prodCounts) => (prod, prodCounts.toArray.sortBy(_._2)(Ordering.Int.reverse).take(nMaxSimsByProd).map(_._1).to)
      }
      writeSimilarities(topCooccByProd.collect(),similarityFile)
    }


    def computeSimilarityFromFeatures(nMaxSims:Int = 20) = {
      //Similarities
      val pFtIdx    = model.productFeatures.zipWithIndex
      val entries   = pFtIdx.flatMap { t => for (i <- t._1._2.indices) yield MatrixEntry(i, t._2, t._1._2(i)) }
      val dic       = pFtIdx.map { t => (t._2, t._1._1) }.collectAsMap()
      val simMatrix = new CoordinateMatrix(entries).toRowMatrix().columnSimilarities(0.1)
      val topSims = simMatrix.entries.top(nMaxSims)(Ordering.by(_.value)).map {
        case MatrixEntry(productIndex1,productIndex2,simLevel) => (dic(productIndex1),dic(productIndex2))
      }
      writeSimilarities(topSims,similarityFile)
    }

    def computeSimilarity(nMaxSims:Int = 20) = {computeSimilarityFromRatings(nMaxSims)}


    def computeRecos(numberOfRecommendation: Int = 3) = {
      @transient val writer = new PrintWriter(recoByUserFile)
      usersByProductCount.collect.foreach{case Row(userId:Int, productCount:Int) =>
        writer.write(userId.toString)
        writer.write(",")
        writer.write(model.recommendProducts(userId, productCount + numberOfRecommendation).map(_.product).drop(productCount).take(numberOfRecommendation).mkString(","))
        writer.write("\n")
      }
      writer.close
    }
}

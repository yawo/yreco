package com.yreco

import java.io.{File, PrintWriter}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.mllib.fpm.AssociationRules.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.mllib.fpm.{FPGrowth, FPGrowthModel}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.Map
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

    /**
     * FETCH, TRANSFORM AND BUILD RATING DATA FROM HBASE STORE. INCREMENTAL LOADING FROM FILE WILL ALSO BE SUPPORTED SOON.
    */
    var ratings: RDD[Rating]        = null
    var productDic:Map[Int,String]  = null

    def loadRatings = {
      val viewRDD: RDD[(ImmutableBytesWritable, Result)] = scProxy.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

      val rawRatings:RDD[(String,String)] = viewRDD.map {
        case (key, res) => Try(Bytes.toString(res.getValue(ProductColumnFamily, ProductIdColumnQualifier))).toOption.map {
          (Bytes.toString(res.getValue(UserColumnFamily, UserIdColumnQualifier)), _)
        }.orNull
      }.filter(_ != null).distinct()
      //Note we dont need to keep the 'inverse' maps in memory
      val productDicInverse  = rawRatings.values.distinct().zipWithIndex().map{case (a,b) =>(a,b.toInt)}.collectAsMap()
      ratings = rawRatings.map{case (a,b) => new Rating(a.toInt,productDicInverse(b),1D)}
      productDic  = productDicInverse.map(_.swap)
    }

    def countByKey[K,V](rdd:RDD[(K,V)])(implicit f:(RDD[(K,V)] => PairRDDFunctions[K,V])): RDD[(K,Int)] ={
      rdd.aggregateByKey[Int](1)((c: Int, v:V) => c+1, (c1: Int, c2: Int) => c1+c2)
    } //or .countApproxDistinctByKey(countApproxAccuracy) but may consume more resources...

    /**
     * BUILD THE RECO MODEL USING ALS ALGORITHM. SEE https://spark.apache.org/docs/latest/mllib-collaborative-filtering.html#collaborative-filtering
     */

    var model: MatrixFactorizationModel             = null
    var usersByProductCount: DataFrame  = null
    var usersByProduct:RDD[(Int,Int)]               = null
    var cooccurences:DataFrame                      = null

    def trainModel = {
        model                       = ALS.trainImplicit(ratings, rank, numIterations)
        usersByProduct              = ratings.map {case Rating(user, product, rate) => (user, product) }.cache()
        val usersByProductCountRDD  = countByKey(usersByProduct);
        usersByProductCount         = usersByProductCountRDD.toDF("usr","pdtCount").cache()

        usersByProductCount.registerTempTable("userbyproductcount")
        computeRecos(usersByProductCountRDD)
        computeSimilarity()
        minePatterns()
    }

    /**
    * COOCURRENCE SIMILARITIES. NAIVE IMPLEMENTAION.
    */
    def computeSimilarity(nMaxSimsByProd:Int = 3,cooccurenceThreshold:Int = 4): Unit ={
      val coocc = countByKey(usersByProduct.join(usersByProduct).collect[((Int,Int),Int)] { case (user, (prod1, prod2)) if prod1 < prod2 => ((prod1, prod2),1)}
        ).reduceByKey{ (a: Int, b: Int) => a + b }.filter(_._2 > cooccurenceThreshold)

      val cooccurencesRDD = coocc.flatMap{ case (prod, count) =>
        Seq((prod._1, (prod._2, count)), (prod._2, (prod._1, count)))
      }.groupByKey.map { case (prod, prodCounts) => (prod, prodCounts.toArray.sortBy(_._2)(Ordering.Int.reverse).take(nMaxSimsByProd).map(_._1))}

      val writer  = new PrintWriter(similarityFile)
      cooccurencesRDD.toLocalIterator.foreach{case (product:Int, sims:Array[Int]) =>
        writer.write(productDic(product))
        writer.write(sims.map(productDic).mkString(",",",","\n"))
      }
      writer.close

      cooccurences = cooccurencesRDD.toDF("product","sims").cache()
      cooccurences.registerTempTable("cooccurences")


    }

    /**
    * PATTERN MINING. see https://spark.apache.org/docs/latest/mllib-frequent-pattern-mining.html
   */
    def minePatterns(minSupport:Double=0.4,numPartitions:Int=10,minConfidence:Double = 0.9): Unit ={
      val usersTransactions:RDD[Array[Int]]  = usersByProduct.groupByKey().map{case (u:Int,tx:Iterable[Int]) => tx.toArray}
      val fpg                                   = new FPGrowth().setMinSupport(minSupport).setNumPartitions(numPartitions)
      val fpModel:FPGrowthModel[Int]            = fpg.run(usersTransactions)

      // Frequent pattern
      val fpWriter = new PrintWriter("/tmp/frequentpatterns.csv")
      fpModel.freqItemsets.sortBy(_.items.size).toLocalIterator.foreach{ itemset: FreqItemset[Int] =>
        fpWriter.write(itemset.items.map(productDic).mkString("[", ",", "] :: "))
        fpWriter.write(itemset.freq.toString)
        fpWriter.write("\n")
      }
      fpWriter.close()

      // Association Rule
      val arWriter = new PrintWriter("/tmp/associationrules.csv")
      fpModel.generateAssociationRules(minConfidence).toLocalIterator.foreach{ rule: Rule[Int] =>
        arWriter.write(rule.antecedent.map(productDic).mkString("[", ",", "] => "))
        arWriter.write(rule.consequent.map(productDic).mkString("[", ",", "] :: "))
        arWriter.write(rule.confidence.toString)
        arWriter.write("\n")
      }
      arWriter.close()
    }

    /**
    * COMPUTE THE RECOS AND STORE THEM IN CSV.
    */
    def computeRecos(usersByProductCountRDD:RDD[(Int,Int)],numberOfRecommendation: Int = 3) = {
        val writer = new PrintWriter(recoByUserFile)
        usersByProductCountRDD.toLocalIterator.foreach{case (userId:Int, productCount:Int) =>
          writer.write(userId.toString)
          writer.write(model.recommendProducts(userId, productCount + numberOfRecommendation).map {
            (x) => productDic(x.product)
          }.slice(productCount, productCount + numberOfRecommendation).mkString(",",",","\n"))
        }
        writer.close
        usersByProductCountRDD //return for chaining
    }
}

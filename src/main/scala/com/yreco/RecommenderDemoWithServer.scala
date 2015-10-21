package com.yreco

import com.twitter.finagle.{ListeningServer, ThriftMux}
import com.twitter.util.{Await, Future}
import thrift.RecommenderEngine.FutureIface
import RecommenderDemo._
/**
 * Created by yawo on 20/09/15.
 *
 *  Submit with:
 *  $SPARK_HOME/bin/spark-submit --class "com.yreco.RecommenderDemo" --master local[4] /home/yawo/Studio/ytech/others/finagle/target/scala-2.11/yreco-assembly-1.0.jar
 */
object RecommenderDemoWithServer {

  def recommendationProxy(userId: Int, numberOfRecommendation: Int, currentItemIds: Seq[Int]): Seq[Int] = {
    println("Got userId %s".format(userId))
    val currentItemIdSeq = if (currentItemIds == null) Seq.empty else currentItemIds
    val previousItemsIdCount:Int = sqlProxy.sql(s"SELECT pdtCount FROM userbyproductcount WHERE usr = ${userId}").collect match {
      case  Array() => 0
      case  x       => x(0).getInt(0)
    }
    val nToRecommend = numberOfRecommendation + currentItemIdSeq.size + previousItemsIdCount
    val recommended = model.recommendProducts(userId, nToRecommend).map(_.product).drop(previousItemsIdCount)
    recommended.diff(currentItemIdSeq).take(numberOfRecommendation)
  }

  def getSimilarProductsProxy(productId: Int): Seq[Int] = {
    sqlProxy.sql(s"SELECT sims FROM cooccurences WHERE product = ${productId}").collect match {
      case  Array() => Seq()
      case  x       => x(0).getAs[Array[Int]](0)
    }
  }

  def main(args: Array[String]) {

    println(
      """
        |starting finagle thriftmux server...
        |You may connect with the following scala code:
        |   import com.twitter.finagle.ThriftMux
        |   import thrift.RecommenderEngine.FutureIface
        |   val client = ThriftMux.newIface[FutureIface](":12000")
      """.stripMargin)

    @transient val server: ListeningServer = ThriftMux.serveIface(":12000", new FutureIface {
      override def getRecommendations(userId: Int, numberOfRecommendation: Int, currentItemIds: Seq[Int]): Future[Seq[Int]] = {
        Future{ recommendationProxy(userId, numberOfRecommendation, currentItemIds) }
      }

      override def reLoadDataAndBuildModel(): Future[Boolean] = {
        Future {
          loadRatings
          trainModel
          true
        }
      }

      override def getSimilarProducts(productId: Int): Future[Seq[Int]] = {
        Future { getSimilarProductsProxy(productId) }
      }
    })

    Await.ready(server)//, Duration.fromSeconds(600)
  }
}

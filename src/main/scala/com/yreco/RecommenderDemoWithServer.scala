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
        recommendationProxy(userId, numberOfRecommendation, currentItemIds)
      }

      override def reLoadDataAndBuildModel(): Future[Boolean] = {
        Future {
          loadRatings
          trainModel
          computeSimilarity()
          computeRecos()
          true
        }
      }
    })
    Await.ready(server)//, Duration.fromSeconds(600)
  }

  def recommendationProxy(userId: Int, numberOfRecommendation: Int, currentItemIds: Seq[Int]): Future[Seq[Int]] = {
    Future {
      println("Got userId %s".format(userId))
      val currentItemIdSeq = if (currentItemIds == null) Seq.empty else currentItemIds
      val previousItemsIdCount = usersByProductCount.get(userId).getOrElse(0)
      val nToRecommend = numberOfRecommendation + currentItemIdSeq.size + previousItemsIdCount
      val recommended = model.recommendProducts(userId, nToRecommend).map(_.product).drop(previousItemsIdCount)
      recommended.diff(currentItemIdSeq).take(numberOfRecommendation)
    }
  }
}

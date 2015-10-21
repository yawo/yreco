package com.yreco

/**
 * Created by yawo on 20/09/15.
 *
 *  Submit with:
 *  $SPARK_HOME/bin/spark-submit --class "com.yreco.RecommenderDemo" --master local[4] /home/yawo/Studio/ytech/others/finagle/target/scala-2.11/yreco-assembly-1.0.jar
 */
import RecommenderDemo._
object RecommenderDemoBatch {

  def main(args: Array[String]) {
    println(
      """
        |*****************************************************************
        |YReco Batch Job. Creating sims.csv and recos.csv...
        |*****************************************************************
        |
      """.stripMargin);
    loadRatings
    trainModel
  }
}

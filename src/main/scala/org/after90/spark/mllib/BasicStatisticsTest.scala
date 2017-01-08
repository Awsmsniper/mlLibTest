package org.after90.spark.mllib

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

/**
  * Created by zhaogj on 04/01/2017.
  */
object BasicStatisticsTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BasicStatisticsTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val observations = sc.parallelize(
      Seq(
        Vectors.dense(1.0, 10.0, 100.0),
        Vectors.dense(2.0, 20.0, 200.0),
        Vectors.dense(3.0, 30.0, 300.0)
      )
    )

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println("mean:" + summary.mean) // a dense vector containing the mean value for each column
    println("variance:" + summary.variance) // column-wise variance
    println("numNonzeros:" + summary.numNonzeros) // number of nonzeros in each column
  }
}

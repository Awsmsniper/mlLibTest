package org.after90.spark.mllib

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 08/01/2017.
  */
object FPGrowthTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPGrowthTest").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val data = sc.textFile("/Users/zhaogj/git/github/apache/spark/data/mllib/sample_fpgrowth.txt")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

    val fpg = new FPGrowth()
      .setMinSupport(0.2)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    val minConfidence = 0.8
    model.generateAssociationRules(minConfidence).collect().foreach { rule =>
      println(
        rule.antecedent.mkString("[", ",", "]")
          + " => " + rule.consequent.mkString("[", ",", "]")
          + ", " + rule.confidence)
    }

    sc.stop
  }
}

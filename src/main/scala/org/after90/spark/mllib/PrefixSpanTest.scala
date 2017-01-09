package org.after90.spark.mllib

import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaogj on 08/01/2017.
  */
object PrefixSpanTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PrefixSpanTest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val sequences = sc.parallelize(Seq(
      Array(Array(1, 2), Array(3)),
      Array(Array(1), Array(3, 2), Array(1, 2)),
      Array(Array(1, 2), Array(5)),
      Array(Array(6))
    ), 2).cache()
    val prefixSpan = new PrefixSpan()
      .setMinSupport(0.5)
      .setMaxPatternLength(5)
    val model = prefixSpan.run(sequences)
    model.freqSequences.collect().foreach { freqSequence =>
      println(
        freqSequence.sequence.map(_.mkString("[", ", ", "]")).mkString("[", ", ", "]") +
          ", " + freqSequence.freq)
    }
  }
}

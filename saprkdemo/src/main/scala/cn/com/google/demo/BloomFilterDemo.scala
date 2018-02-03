package cn.com.google.demo

import bloomfilter.mutable.BloomFilter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by moses on 2017/8/14.
  */
object BloomFilterDemo {
  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
    //val sc = new SparkContext(new SparkConf().setAppName("aa").setMaster("local[*]"))
    val expectedElements = 1000
    val falsePositiveRate: Double = 0.1
    val bf = BloomFilter[String](expectedElements, falsePositiveRate)
    bf.add("some string")
    bf.mightContain("some string")
    bf.dispose()
  }
}

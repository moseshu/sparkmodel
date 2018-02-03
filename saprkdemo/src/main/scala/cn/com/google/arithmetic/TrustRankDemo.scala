package cn.com.google.arithmetic

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by moses on 2017/11/30.
  */
object TrustRankDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    import scala.collection.JavaConverters._
    val conf=new SparkConf().setAppName("TrustRank").setMaster("local[*]")
    val sc=new SparkContext(conf)
    val data =sc.textFile("d://r.txt")
    val call=data.map{x=>
      val fields=x.split(",")
      (fields(0),fields(1))
    }
    val bad=sc.textFile("d://bad.txt").collect().toSet.asJava
    val good=sc.textFile("d://good.txt").collect().toSet.asJava

    val out=call.combineByKey(mutable.Set(_),(x:mutable.Set[String],y:String)=>x + y,(x:mutable.Set[String],y:mutable.Set[String])=>x ++ y).
      collect().toMap.map(x=>(x._1,x._2.asJava)).asJava

    val in=call.map(x=>(x._2,x._1)).combineByKey(mutable.Set(_),(x:mutable.Set[String],y:String)=>x + y,(x:mutable.Set[String],y:mutable.Set[String])=>x ++ y).
      collect().toMap.map(x=>(x._1,x._2.asJava)).asJava
    val userSet=mutable.Set[String]().empty
    call.collect().foreach{x=>
      userSet += x._1
      userSet += x._2
    }
    println(s"suerSize=${userSet.size}")
    val trustRank=new TrustRank(out,in,userSet.asJava);
    trustRank.setBadSeeds(bad)
    trustRank.setGoodSeeds(good)
    val result=trustRank.run().asScala
    trustRank.getBadScore.asScala.foreach(println(_))
    /*val b=result.map{x=>
      val re=if(x._2==0.0) (x._1->java.lang.Double.parseDouble("1.0")) else (x._1->java.lang.Double.parseDouble(x._2+""))
      re
    }
    val a=for{(k,v) <- b}yield (k,(-1)*Math.log(v))*/
    println("****************************************************")
    //result.foreach(println(_))
  }

}

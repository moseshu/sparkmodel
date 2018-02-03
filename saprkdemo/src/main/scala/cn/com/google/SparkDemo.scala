package cn.com.google

import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by ThinkPad on 2017/7/20.
  */
object SparkDemo {
  def main(args: Array[String]): Unit = {
    /*Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val conf = new SparkConf().setMaster("local[3]").setAppName("GrapfDemo")
    val sc = new SparkContext(conf)*/
    /*val sort=sc.parallelize(List((3,"java"),(4,"spark"),(1,"hadoop")))
    sort.sortByKey(false).collect().foreach(println(_))*/
    // Create an RDD for the vertices
    //testPageRank(sc)
    //testGraphx(sc)
    //testDemo(sc)
    //   testPhone(sc)
    //    testSortBy(sc)
    //testmap()
//    testStructuredStreaming(sc)

  }
def testStructuredStreaming(sc:SparkContext): Unit ={
  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .getOrCreate()

  import spark.implicits._
  val lines = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
  val words = lines.as[String].flatMap(_.split(" "))
  val wordCounts = words.groupBy("value").count()
  val query = wordCounts.writeStream
    .outputMode("complete")
    .format("console")
    .start()

  query.awaitTermination()


}
  def testPageRank(sc: SparkContext): Unit = {
    val links = sc.parallelize(List(("A", List("B", "C")), ("B", List("A", "C")), ("C", List("A", "B", "D")), ("D", List("C")))).partitionBy(new HashPartitioner(100)).persist()

    var ranks = links.mapValues(v => 1.0)
    for (i <- 0 until 10) {
      val contributions = links.join(ranks).flatMap {
        case (pageId, (links, rank)) => links.map(dest => (dest, rank / links.size))
      }
      ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => 0.15 + 0.85 * v)

    }
    ranks.sortByKey().collect().foreach(println(_))
  }


  def testGraphx(sc: SparkContext): Unit = {
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
        (4L, ("peter", "student"))))
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(7L, 5L, "pi"),
        Edge(4L, 0L, "student"), Edge(5L, 0L, "colleague")))
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Notice that there is a user 0 (for which we have no information) connected to users
    // 4 (peter) and 5 (franklin).
    /*graph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1
    ).collect.foreach(println(_))*/
    //graph.triplets.map(triplet=>triplet.attr).collect().foreach(println(_))
    graph.triplets.collect.foreach(println(_))

    //graph.edges.map(ed=>ed.attr).collect().foreach(println(_))
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    // validGraph.vertices.collect.foreach(println(_))

    val inputGraph: Graph[Int, String] =
      graph.outerJoinVertices(graph.outDegrees)((vid, _, degOpt) => degOpt.getOrElse(0))
    inputGraph.triplets.foreach(println(_))
    val outputGraph: Graph[Double, Double] =
      inputGraph.mapTriplets(triplet => 1.0 / triplet.srcAttr).mapVertices((id, _) => 1.0)
    val strongconn = graph.stronglyConnectedComponents(100)
    strongconn.triplets.collect().foreach(println(_))

  }

  def testDemo(sc: SparkContext): Unit = {
    val vertexArray = Array(
      (1L, ("Alice", 28)),
      (2L, ("Bob", 27)),
      (3L, ("Charlie", 65)),
      (4L, ("David", 42)),
      (5L, ("Ed", 55)),
      (6L, ("Fran", 50))
    )
    val edgeArray = Array(
      Edge(2L, 1L, 7),
      Edge(2L, 4L, 2),
      Edge(3L, 2L, 4),
      Edge(3L, 6L, 3),
      Edge(4L, 1L, 1),
      Edge(5L, 2L, 2),
      Edge(5L, 3L, 8),
      Edge(5L, 6L, 3)
    )
    val vertexRDD = sc.parallelize(vertexArray)
    val edgeRDD = sc.parallelize(edgeArray)
    val graph = Graph(vertexRDD, edgeRDD)
    //val pageGraph=graph.pageRank(0.001).vertices
    val neibothID=graph.collectNeighborIds(EdgeDirection.In)
    // neibothID.collect().foreach(x=>println(x._1+"'s in neibor  "+x._2.mkString(",")))
    println("----------------------------")
    val inneibor=graph.collectNeighbors(EdgeDirection.In)
    inneibor.collect().foreach(x=>println(x._1+"'s in neibor  "+x._2.mkString(",")))
    val outneibor=graph.collectNeighbors(EdgeDirection.Out)
    println("----------------------------------")
    //    outneibor.collect().foreach(x=>println(x._1+"'s in neibor  "+x._2.mkString(",")))
    println("---------------------------------------")
    val inout=inneibor.join(outneibor).map(x=>(x._2._1.mkString(","),(x._1,x._2._2.mkString(","))))
    //    inout.collect().foreach(x=>println(x._1.split(",")(0)+"'s in neibor  "+x._2))
    graph.mapTriplets(x => (1.0) ).triplets.collect().foreach(println(_))
    //pageGraph.foreach(println(_))
    //graph.vertices.collect().foreach(println(_))
    //    val strongconn=graph.stronglyConnectedComponents(100).partitionBy(PartitionStrategy.RandomVertexCut)
    //strongconn.vertices.collect().foreach(println(_))
    //    graph.inDegrees.foreach(println(_))
    /* val in=graph.joinVertices(graph.inDegrees){(id,defaultAttr,inDeg)=>(defaultAttr._1,inDeg)}
     val out=graph.joinVertices(graph.outDegrees){(id,defaultAttr,outDeg)=>(defaultAttr._1,outDeg)}
     val res=in.joinVertices(out.vertices){(id,defaultAttr,out)=>(defaultAttr._1,out._2*defaultAttr._2)}
     val a=res.subgraph(vpred = (id,attr)=>attr._2>3)*/
    //a.vertices.foreach(println(_))
    //    strongconn.edges.foreach(println(_))

    //    strongconn.vertices.foreach(println(_))

    //println(graph.numVertices)
    val sourceId: VertexId = 5L
    /*val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(

      (id, dist, newDist) => math.min(dist, newDist),

      triplet => {  // 计算权重

        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {

          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))

        } else {

          Iterator.empty

        }

      },

      (a,b) => math.min(a,b) // 最短距离

    )

    println(sssp.vertices.collect.mkString("\n"))*/
    sc.stop()
  }

  def testPhone(sc: SparkContext): Unit = {
    val rdd = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\combined.txt").map(x => (x.split(",")(0).toLong, x.split(",")(1).toLong)).cache()
    val rdd1 = rdd.map(x => (x._1, 1)).reduceByKey(_ + _)
    val rdd2 = rdd.map(x => (x._2, 1)).reduceByKey(_ + _)
    val total = rdd1.union(rdd2).foldByKey(0)(_ + _).filter(x => x._2 > 1)
    val kvcount = rdd.join(total).map { case (k, v) => (v._1, ((k, v._1), v._2)) }
    val resultdRDD = kvcount.join(total).values.keys.keys
    val graph = Graph.fromEdgeTuples(resultdRDD, 0)
    val pageGraph = graph.pageRank(0.001).persist(StorageLevel.MEMORY_AND_DISK_SER_2)
    pageGraph.vertices.foreach(println(_))
    /*val in=graph.joinVertices(graph.inDegrees){(id,attr,inDeg)=>inDeg}
    val out=graph.joinVertices(graph.outDegrees){(id,attr,outDeg)=>outDeg}
    val remove0Ege=in.joinVertices(out.vertices){(id,in,out)=>out*in}
    val subgraph=remove0Ege.subgraph(vpred = (id,attr)=>attr!=0)*/

  }

  def testGetOne2(sc: SparkContext): Unit = {
    val rdd = sc.textFile("C:\\Users\\ThinkPad\\Desktop\\combined.txt").map(x => (x.split(",")(0).toLong, x.split(",")(1).toLong))
    val rdd1 = rdd.map(x => (x._1, 1)).reduceByKey(_ + _)
    //key为主叫，value为一度联系人个数
    val rdd2 = rdd.map(x => (x._2, 1)).reduceByKey(_ + _)
    val twoDegree = rdd.map(x => (x._2, x._1)).join(rdd2).map(x => x._2)
    //key为主叫，value为二度联系人个数
    val total = rdd1.union(rdd2).foldByKey(0)(_ + _).filter(x => x._2 > 1)
    val kvcount = rdd.join(total).map { case (k, v) => (v._1, ((k, v._1), v._2)) }
    val resultdRDD = kvcount.join(total).values.keys.keys
    val oneauto = rdd.combineByKey(List(_), (x: List[Long], y: Long) => y :: x, (x: List[Long], y: List[Long]) => x ::: y)
    val twoauto = rdd.map(x => (x._2, x._1)).combineByKey(List(_), (x: List[Long], y: Long) => y :: x, (x: List[Long], y: List[Long]) => x ::: y)
    oneauto.map { x =>
      val list = x._2
      twoauto.map { c =>
        if (list.contains(c._1)) (x, c)
      }
    }
    val graph = Graph.fromEdgeTuples(rdd, 0).mapVertices((id, _) => id.toLong)
    graph.mapTriplets { triplet =>
      triplet.srcAttr
    }
    val indegreegraph = graph.collectNeighborIds(EdgeDirection.In)

  }

  def testSortBy(sc: SparkContext): Unit = {
    val rdd = sc.parallelize(List(("hadoop", 2), ("fd", 3)))
    //rdd.sortBy(x => x._2, false).foreach(println(_))
    val random=new Random()
    val char=random.nextPrintableChar()
    println(char)

  }
  def testmap(): Unit = {

    val set =(0 to 10000000).map(x=>x.toString).toSet
    set.foreach(println(_))
  }
}

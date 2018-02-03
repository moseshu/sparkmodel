package cn.com.google

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ThinkPad on 2017/8/4.
  */
object GraphDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext(new SparkConf().setAppName("aa").setMaster("local[*]"))
    //    val friendsGraph = GraphLoader.edgeListFile(sc, "E:/project/data/combined1.txt")
    val edge = List(//边的信息
      (1, 2), (1, 3), (2, 3), (3, 4), (3, 5), (3, 6),
      (4, 5), (5, 6), (7, 8), (7, 9), (8, 9))

    //    //构建边的rdd
    val edgeRdd = sc.parallelize(edge).map(x => {
      Edge(x._1.toLong, x._2.toLong, None)
    })

    //构建图 顶点Int类型
    val g = Graph.fromEdges(edgeRdd, 0)
    g.pageRank(0.001).cache()
    //g.triplets.collect().foreach(println(_))
    val two = 2
    //这里是二跳邻居 所以只需要定义为2即可
    val newG = g.mapVertices((vid, _) => Map[VertexId, Int](vid -> two))
      .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)
      newG.vertices.collect().foreach(println(_))


  }

  type VMap = Map[VertexId, Int]

  /**
    * 节点数据的更新 就是集合的union
    */
  def vprog(vid: VertexId, vdata: VMap, message: VMap): Map[VertexId, Int] = addMaps(vdata, message)

  /**
    * 发送消息
    */
  def sendMsg(e: EdgeTriplet[VMap, _]) = {
    //取两个集合的差集  然后将生命值减1
    val srcMap = (e.dstAttr.keySet -- e.srcAttr.keySet).map { k => k -> (e.dstAttr(k) - 1) }.toMap
    val dstMap = (e.srcAttr.keySet -- e.dstAttr.keySet).map { k => k -> (e.srcAttr(k) - 1) }.toMap

    if (srcMap.isEmpty && dstMap.isEmpty)
      Iterator.empty
    else
      Iterator((e.dstId, dstMap), (e.srcId, srcMap))
  }

  /**
    * 消息的合并
    */
  def addMaps(spmap1: VMap, spmap2: VMap): VMap =
    (spmap1.keySet ++ spmap2.keySet).map {
      k => k -> math.min(spmap1.getOrElse(k, Int.MaxValue), spmap2.getOrElse(k, Int.MaxValue))
    }.toMap


  //这里是二跳邻居 所以只需要定义为2即可
  /*val newG = friendsGraph.mapVertices((vid, _) => Map[VertexId, Int](vid -> two))
    .pregel(Map[VertexId, Int](), two, EdgeDirection.Out)(vprog, sendMsg, addMaps)
  newG.vertices.map(x => {
    val k = x._1
    val v = s"(${x._2.mkString(",")})"
    (k, v)
  }).repartition(1).saveAsTextFile("E:/project/data/results")

  //    newG.vertices.take(10).foreach(println(_))

  val twoJumpFirends = newG.vertices
    .mapValues(_.filter(_._2 == 0).keys)*/


}

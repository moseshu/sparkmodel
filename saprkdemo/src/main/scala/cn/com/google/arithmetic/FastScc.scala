package cn.com.google.arithmetic

/**
  * Created by moses on 2017/11/30.
  */
class FastScc {
  /**
    * strongly_connected_components单机版。效率比分布式高。
    * 参考： https://github.com/networkx/networkx/blob/master/networkx/algorithms/components/strongly_connected.py
    * @param edges
    * @return [Long, Set],Long为group id（值为group成员里的最小值），Set为这个group里全部成员的集合。
    */
  def fastScc(edges: Array[(Long, Long)]): Map[Long, scala.collection.mutable.Set[Long]] = {
    val vertexs =  scala.collection.mutable.Set[Long]() 	//全部的顶点
    val nbrs = scala.collection.mutable.Map[Long, java.util.LinkedList[Long]]();	//邻居表
    edges.foreach( tuple=> {
      val num = tuple._1
      val other_num = tuple._2
      vertexs += num
      vertexs += other_num
      var nrs = new java.util.LinkedList[Long]()
      if(nbrs.contains(num))
        nrs = nbrs.get(num).get
      else
        nbrs.put(num, nrs)
      nrs.add(other_num)
    }
    )

    val res = scala.collection.mutable.Map[Long, scala.collection.mutable.Set[Long]]()
    val preorder = scala.collection.mutable.Map[Long,Integer]()
    val lowlink = scala.collection.mutable.Map[Long,Integer]()
    val scc_found  = scala.collection.mutable.Set[Long]()
    val scc_queue = new java.util.LinkedList[Long]()
    var i = 0;	//Preorder counter
    for( source <- vertexs){
      if(!scc_found.contains(source )){
        val queue = scala.collection.mutable.ListBuffer[Long]()
        queue += source
        while(!queue.isEmpty){
          val v = queue(queue.length - 1)
          if(!preorder.contains(v)){
            i = i + 1;
            preorder.put(v, i);
          }
          var done = 1;
          val v_nbrs = nbrs.get(v);
          if(!v_nbrs.isEmpty){
            import scala.util.control.Breaks._
            import scala.collection.JavaConversions._  //http://stackoverflow.com/questions/17459674/how-can-i-use-a-java-list-with-scalas-foreach
            breakable{for( w <- v_nbrs.get){
              if(!preorder.contains(w)){
                queue += w
                done = 0;
                break;
              }
            }
            }
          }
          if(done == 1){
            lowlink.put(v, preorder(v));
            if(!v_nbrs.isEmpty){
              import scala.collection.JavaConversions._
              for(w <- v_nbrs.get){
                if(!scc_found.contains(w)){
                  if(preorder(w) > preorder(v))
                    lowlink.put(v, Math.min(lowlink(v), lowlink(w)));
                  else
                    lowlink.put(v, Math.min(lowlink(v), preorder(w)));
                }
              }
            }
            queue.remove(queue.length - 1);
            val y = lowlink.get(v)
            val z = preorder.get(v)
            if(y == z){
              scc_found.add(v);
              val scc = scala.collection.mutable.Set[Long]()
              var gid = v
              scc += v
              while(!scc_queue.isEmpty() && (preorder.get(scc_queue.get(scc_queue.size() - 1)).get > preorder.get(v).get)){
                val k = scc_queue.get(scc_queue.size() - 1);
                scc_queue.remove(scc_queue.size() - 1);
                scc_found += k
                scc += k
                if(k < gid)
                  gid = k
              }
              res += (gid -> scc)
            }else{
              scc_queue.add(v);
            }
          }
        }
      }
    }
    res.toMap
  }
}

object FastScc{

  def main(args: Array[String]) {
    test()
  }

  def test(){
    //    val file = "D:\\dev\\graph\\exported\\scc_newonly_170508\\new_only\\wholeData\\combined.txt";
    val file = "C:\\Users\\ThinkPad\\Desktop\\aa.txt";
    var sc = new java.util.Scanner(new java.io.File(file));
    var line = 0;
    while(sc.hasNextLine()){
      line = line + 1;
      sc.nextLine();
    }
    line = 0;
    sc = new java.util.Scanner(new java.io.File(file));
    val input = scala.collection.mutable.ArrayBuffer[(Long, Long)]()
    while(sc.hasNextLine()){
      val ll = sc.nextLine().split(",")
      input += ((ll(0).toLong,ll(1).toLong))
    }
    val res = new FastScc().fastScc(input.toArray);
    res.foreach(println(_))
  }
}
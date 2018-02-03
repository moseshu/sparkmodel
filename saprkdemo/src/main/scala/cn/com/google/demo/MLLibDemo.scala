package cn.com.google.demo

import breeze.linalg.{DenseMatrix, diag}

/**
  * Created by ThinkPad on 2017/8/25.
  */
object MLLibDemo {
  def main(args: Array[String]): Unit = {
    val v10=DenseMatrix.tabulate(3,2){case (i,j)=>i + j}
    //println(v10)
    val m=DenseMatrix((1.0,2.0,3.0),(4.0,5.0,6.0),(7.0,8.0,9.0))

    val b=DenseMatrix((1.0,1.0,1.0),(1.0,1.0,1.0),(1.0,1.0,1.0))
    val array1=Array("hello","spark")
    val array2=Array("hadoop","scala","hello")
    val tmp=(array1 ++ array2).toSet
   //tmp.foreach(println(_))
    //println(m \ b )
    val tupe=List(("a",1),("a",2),("b",3),("c",4),("b",5))

  }



}

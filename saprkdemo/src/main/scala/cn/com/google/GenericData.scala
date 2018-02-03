package cn.com.google

import java.io.PrintWriter
import java.net.ServerSocket

import scala.collection.mutable.ArrayBuffer
import scala.io.StdIn
import scala.util.Random

/**
  * Created by ThinkPad on 2017/8/7.
  */
object GenericData {
  def main(args: Array[String]): Unit = {
    val data=ArrayBuffer[String](
      "This lines DataFrame represents an unbounded table containing the streaming text data",
      "This table contains one column of strings named “value”",
      "and each line in the streaming",
      "text data becomes a row",
      "in the table. Note, that this is not currently",
      "receiving any data as we are just ",
      "setting up the transformation"
    )


    val listener = new ServerSocket(9999)
    val random=new Random()
    while (true){
      println("hello")
      val index=random.nextInt(8)
      val socket = listener.accept()
      new Thread(){
        override def run(): Unit = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
            Thread.sleep(10000)
            out.write(data(index))
            out.flush()
          socket.close()
        }
      }.start()
    }

  }


}

package cn.com.google.actor

import akka.actor.{Actor, ActorContext, ActorSystem, Props}
import akka.actor.Actor.Receive
import com.typesafe.config.ConfigFactory

/**
  * Created by ThinkPad on 2017/8/24.
  */
case object ChenLei{
  def readData(name:String): Unit ={
    println("Hello "+name)
  }
}
case object LiuWei{
  def sendData(name:String): Unit ={
    println("Tell XP! Let "+name +" give back the bicycle")
  }
}
case class XiongPeng(){
  def hitYou(name:String): Unit ={
    println("I hate "+name+ " haha.. joking")
  }
}
class HiActor(name:String) extends Actor {
  override def receive: Receive = {
    case "HI" => println("hello")
    case "scala" => {
      println("you are the best")
    }
    case ChenLei => {
      ChenLei.readData(name)
    }
    case xp:XiongPeng=>{
      xp.hitYou(name)
    }
    case LiuWei=>LiuWei.sendData(name)
  }
}

object HiActor {
  def main(args: Array[String]): Unit = {
    val system = ActorSystem()
    val myactor = system.actorOf(Props(classOf[HiActor],"moses"))
    myactor ! ChenLei
    myactor ! LiuWei
    myactor ! new XiongPeng()

  }
}


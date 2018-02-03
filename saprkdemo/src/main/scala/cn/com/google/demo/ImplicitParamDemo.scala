package cn.com.google.demo

/**
  * Created by moses on 2017/11/22.
  */
object ImplicitParamDemo {
  object Greeter{
    def greet(name:String)(implicit prompt: String) {
      println("Welcome, " + name + ". The System is ready.")
      println(prompt)
    }
  }
  def main(args: Array[String]) {
    implicit val prompt = ">"
    Greeter.greet("admin")
  }
}

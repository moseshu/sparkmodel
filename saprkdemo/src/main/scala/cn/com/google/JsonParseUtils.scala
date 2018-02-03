package cn.com.google


import org.json4s.{DefaultFormats, JDouble}
import org.json4s.JsonAST.{JArray, JDecimal, JInt, JString}
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

import scala.util.Random

/**
  * Created by ThinkPad on 2017/8/4.
  */
object JsonParseUtils {
  def main(args: Array[String]): Unit = {
    val json = "{\"name\":\"BeJson\",\"url\":\"http://www.bejson.com\",\"page\":88,\"isNonProfit\":true,\"address\":{\"street\":\"科技园路.\",\"city\":\"江苏苏州\",\"country\":\"中国\"},\"links\":[{\"name\":\"Google\",\"url\":\"http://www.google.com\"},{\"name\":\"Baidu\",\"url\":\"http://www.baidu.com\"},{\"name\":\"SoSo\",\"url\":\"http://www.SoSo.com\"}]}"
    json2Map(json)
val rand=new Random()
//println(rand.nextInt(9))
jsonstudy()
  }
  def jsonstudy(): Unit ={

    val a = parse(""" { "numbers" : [1, 2, 3, 4],"test":[{"count":12.34,"scala":"Moses"},{"count":20.30,"scala":"Anne"}] },"result":null """,useBigDecimalForDouble = true)
    val res=a \\ "test"
    val str=compact(res(0) \ "count")
    println(str)
    /*for(r <- res){
      println(r)
    }
    println()*/
    /*val js=a.values
    println(res)
    println(js)
    println(compact(a))
    val b = parse("""{"name":"Toy","price":35.35}""", useBigDecimalForDouble = true)
    println(compact(render(b)))

    val c = List(1, 2, 3)
    val d = compact(render(c))
    println(d)
    val e = ("name" -> "joe")
    val f = compact((render(e)))
    println(f)
    val g = ("name" -> "joe") ~ ("age" -> 35)
    val h = compact(render(g))
    println("h="+h)
    val i = ("name" -> "joe") ~ ("age" -> Some(35))
    val j = compact(render(i))
    println("j="+j)
    val k = ("name" -> "joe") ~ ("age" -> (None: Option[Int]))
    val l = compact(render(k))
    println("l="+l)

    //定义json
    println("===========================")
    //推荐这种方式，因为可以用在使用map
    val jsonobj = (
      ("name" -> "xiaoming") ~
        ("age" -> 12)
      )
    println(jsonobj)
    println(compact(render(jsonobj)))

    val jsonobjp = parse(
      """{
            "name":"xiaogang",
            "age":12
          }""")
    println(jsonobjp)
    val aa=render(jsonobjp)
    //aa(0)
    println(compact(aa))

    //通过类生成json
    println("===========================")
    case class Winner(id: Long, numbers: List[Int])
    case class Lotto(id: Long, winningNumbers: List[Int], winners: List[Winner], drawDate: Option[java.util.Date])
    val winners = List(Winner(23, List(2, 45, 34, 23, 3, 5)), Winner(54, List(52, 3, 12, 11, 18, 22)))
    val lotto = Lotto(5, List(2, 45, 34, 23, 7, 5, 3), winners, None)
    val json =
      ("lotto" ->
        ("lotto-id" -> lotto.id) ~
          ("winning-numbers" -> lotto.winningNumbers) ~
          ("draw-date" -> lotto.drawDate.map(_.toString)) ~
          ("winners" ->
            lotto.winners.map { w =>
              (("winner-id" -> w.id) ~
                ("numbers" -> w.numbers))
            }))

    println(compact(render(json)))*/
  }
  def json2Map(json: String) {
    implicit val formats = DefaultFormats
    //解析结果

    val responseInfo: ResponseInfo = parse(json).extract[ResponseInfo]
    println(responseInfo)

    println("************************************************")

    //数组数据
    val addressArray = responseInfo.address
    val linkArray = responseInfo.links
    println(addressArray)
    for (link <- linkArray) {
      println("linkArray：" + link)
    }

  }

}
case class AddressInfo(street: String, city: String, country: String) {
  override def toString = s"street:$street,  city:$city,  county:$country"
}

case class LinkInfo(name: String, url: String) {
  override def toString = s"name:$name,  url:$url"
}

//一级列表
case class ResponseInfo(name: String, url: String, page: Integer,
                        isNonProfit: Boolean, address: AddressInfo,
                        links: Array[LinkInfo]) {
  val lis:String=links.mkString("\n")
  override def toString = s"name:$name,  url:$url, page:$page,  isNonProfit:$isNonProfit,  address:$address,  links:$lis"
}


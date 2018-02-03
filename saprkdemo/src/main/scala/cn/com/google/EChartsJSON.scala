package cn.com.google

import scala.collection.mutable.ListBuffer

/**
  * Created by ThinkPad on 2017/8/9.
  */
case class EChartsJSON(var nodes:ListBuffer[EChartsJsonNode],
                        var links:ListBuffer[EChartsJsonLink]
                      )extends Serializable{

}
case class EChartsJsonNode(val name: String,   //电话号
                           val attributes: List[String],  //属性，暂时不用
                           val size: Int,  //节点大小。当前为度的值。
                           val id: String,  //电话号
                           val category: Int,//0:非新发现， 1:新发现
                           val category_name: String  //  和category一一对应。为UI上直接显示的字符
                          ) extends Serializable
case class EChartsJsonLink(
                           val source: String, //number
                           val target: String  //other number
                         ) extends Serializable{
}

package cn.com.google.demo

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.json4s.JsonDSL._

import scala.collection.mutable
import scala.io.Source

/**
  * Created by ThinkPad on 2017/9/26.
  */

object ParseJson {
  val process_frequency_one_dim: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }
  }

  val process_frequency_count: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val sub_dim_type = compact(condition \ "sub_dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type-$sub_dim_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }

  }

  val process_frequency_distinct: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_frequency_count(prefix, conditions, res)
  }

  val process_black_list: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      for (hit <- (condition \\ "hits").children) {
        val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
        val fraud_type_display_name = compact(hit \ "fraud_type_display_name").replaceAll("\"","")
        val key = s"$prefix-$dim_type-$fraud_type_display_name"
        var value = res.getOrElseUpdate(key, "0").replaceAll("\"","").toInt
        value += 1
        res(key) = value + ""
      }
    }
  }

  val process_fp_exception: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val code_display_name = compact(condition \ "code_display_name").replaceAll("\"","")
      val key = s"$prefix-$code_display_name"
      var value = res.getOrElseUpdate(key, "0").replaceAll("\"","").toInt
      value += 1
      res(key) = value + ""
    }
  }

  val process_grey_list: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_black_list(prefix, conditions, res)
  }

  val process_fuzzy_black_list: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_black_list(prefix, conditions, res)
  }

  val process_geo_ip_distance: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type"
      var value = compact(condition \ "result").replaceAll("\"","")
      value = if (value == "") "0" else value
      val unit = compact(condition \ "unit").replaceAll("\"","")
      if (unit == "m")
        value = f"${value.toFloat / 1000}%1.2f"
      res += (key -> value)
    }
  }

  val process_proxy_ip: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val proxy_ip_type = compact(condition \ "proxy_ip_type").replaceAll("\"","")
      val key = s"$prefix-$proxy_ip_type"
      var value = res.getOrElseUpdate(key, "0").replaceAll("\"","").toInt
      value += 1
      res(key) = value + ""
    }
  }

  val process_match_address: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    val value = conditions.children.size + ""
    res += (prefix -> value)
  }

  val process_gps_distance: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val gps_a = compact(condition \ "gps_a").replaceAll("\"","")
      val gps_b = compact(condition \ "gps_b").replaceAll("\"","")
      val key = s"$prefix-$gps_a-$gps_b"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }
  }

  val process_regex: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type"
      res += (key -> "1")
    }
  }

  val process_event_time_diff: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (prefix -> value)
    }
  }

  val process_time_diff: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_event_time_diff(prefix, conditions, res)
  }

  val process_active_days_two: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_frequency_count(prefix, conditions, res)
  }

  val process_cross_event: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_frequency_count(prefix, conditions, res)
  }

  val process_cross_velocity_one_dim: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val match_dim_type = compact(condition \ "match_dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type-$match_dim_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }
  }

  val process_cross_velocity_count: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val match_dim_type = compact(condition \ "match_dim_type").replaceAll("\"","")
      val sub_dim_type = compact(condition \ "sub_dim_type")
      val key = s"$prefix-$dim_type-$sub_dim_type-$match_dim_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }
  }

  val process_cross_velocity_distinct: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_cross_velocity_count(prefix, conditions, res)
  }

  val process_calculate: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val calc_type = compact(condition \ "calc_type").replaceAll("\"","")
      val sub_dim_type = compact(condition \ "sub_dim_type").replaceAll("\"","")
      val key = s"$prefix-$dim_type-$sub_dim_type-$calc_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
    }
  }

  val process_last_match: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_frequency_one_dim(prefix, conditions, res)
  }

  val process_min_max: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_calculate(prefix, conditions, res)
  }

  val process_count: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_frequency_count(prefix, conditions, res)
  }

  val process_association_partner: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (prefix -> value)
      for (hit <- (condition \\ "hits").children) {
        val industry_display_name = compact(hit \ "industry_display_name").replaceAll("\"","")
        val key = s"$prefix-$industry_display_name"
        val count = compact(hit \ "count").replaceAll("\"","")
        res += (key -> count)
      }
      for (hits_for_dim <- (condition \\ "hits_for_dim").children) {
        val dim_type = compact(hits_for_dim \ "dim_type").replaceAll("\"","")
        val industry_display_name = compact(hits_for_dim \ "industry_display_name").replaceAll("\"","")
        val count = compact(hits_for_dim \ "count").replaceAll("\"","")
        val key = s"$prefix-$dim_type-$industry_display_name"
        res += (key -> count)
      }
      for (result_for_dim <- (condition \\ "results_for_dim").children) {
        val dim_type = compact(result_for_dim \ "dim_type").replaceAll("\"","")
        val key = s"$prefix-$dim_type"
        val count = compact(result_for_dim \ "count").replaceAll("\"","")
        res += (key -> count)
      }
    }
  }

  val process_discredit_count: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val calc_dim_type = compact(condition \ "calc_dim_type").replaceAll("\"","")
      val calc_type = compact(condition \ "calc_type").replaceAll("\"","")
      var value = compact(condition \ "result").replaceAll("\"","")
      value = if (value == "") "0" else value
      val key = s"$prefix-$calc_dim_type-$calc_type"
      res += (key -> value)
      for (hit <- (condition \\ "hits").children) {
        val overdue_amount = compact(hit \ "overdue_amount").replaceAll("\"","")
        val key = if (overdue_amount == "") s"$prefix-$calc_dim_type-$calc_type-$overdue_amount" else s"""$prefix-$calc_dim_type-$calc_type-"-1""""
        if (res.keySet.contains(key))
          res(key) = (res(key).toInt + 1) + ""
        else res(key) = value
      }
    }
  }

  val process_cross_partner: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val event_type = compact(condition \ "event_type").replaceAll("\"","")
      val key = s"$prefix-$event_type"
      val value = compact(condition \ "result").replaceAll("\"","")
      res += (key -> value)
      for (hit <- (condition \\ "hits").children) {
        val industry_display_name = compact(hit \ "industry_display_name").replaceAll("\"","")
        val key = s"$prefix-$industry_display_name"
        val count = compact(hit \ "count").replaceAll("\"","")
        res += (key -> count)
      }
      for (hit <- (condition \\ "hits_for_dim").children) {
        val original_dim_type = compact(hit \ "original_dim_type").replaceAll("\"","")
        val match_dim_type = compact(hit \ "match_dim_type")
        val industry_display_name = compact(hit \ "industry_display_name").replaceAll("\"","")
        val key = s"$prefix-$original_dim_type-$match_dim_type-$industry_display_name"
        val v = compact(hit \ "count").replaceAll("\"","")
        res += (key -> v)
      }
      for (hit <- (condition \\ "results_for_dim").children) {
        val original_dim_type = compact(hit \ "original_dim_type").replaceAll("\"","")
        val match_dim_type = compact(hit \ "match_dim_type").replaceAll("\"","")
        val k = s"$prefix-$original_dim_type-$match_dim_type"
        val v = compact(hit \ "count").replaceAll("\"","")
        res += (k-> v)
      }
    }
  }

  val process_four_calculation: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_event_time_diff(prefix, conditions, res)
  }

  val process_function_kit: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_event_time_diff(prefix, conditions, res)
  }

  val process_usual_browser: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type").replaceAll("\"","")
      val v = compact(condition \ "result").replaceAll("\"","")
      val unit = compact(condition \ "unit").replaceAll("\"","")
      val k = s"$prefix-$dim_type-$unit"
      res += (k -> v)
    }
  }

  val process_usual_device: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_usual_browser(prefix, conditions, res)
  }

  val process_usual_location: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_usual_browser(prefix, conditions, res)
  }

  val process_keyword: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      for (keyword <- condition \ "data" \\ classOf[JString]) {
        val k = s"$prefix-$keyword"
        var v = res.getOrElseUpdate(k, "0").replaceAll("\"","").toInt
        v += 1
        res(k) = v + ""
      }
    }
  }

  val process_android_cheat_app: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (i <- 0 to conditions.children.size) {
      var v = res.getOrElseUpdate(prefix, "0").replaceAll("\"","").toInt
      v += 1
      res(prefix) = v + ""
    }
  }

  val process_ios_cheat_app: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_android_cheat_app(prefix, conditions, res)
  }

  val process_android_emulator: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    process_android_cheat_app(prefix, conditions, res)
  }

  val process_device_status_abnormal: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    for (condition <- conditions.children) {
      val abnormal_tags = (condition \\ "abnormal_tags").children.size
      if (res.keySet.contains(prefix))
        res(prefix) = (res(prefix).toInt + abnormal_tags) + ""
      else res(prefix) = abnormal_tags + ""
    }
  }

  val process_suspected_team: (String, JValue, mutable.Map[String, String]) => Unit = (prefix: String, conditions: JValue, res: mutable.Map[String, String]) => {
    val d = List[String](
      "total_cnt",
      "group_id",
      "black_cnt",
      "grey_cnt",
      "black_rat",
      "grey_rat",
      "degree",
      "total_cnt_two",
      "black_cnt_one",
      "black_cnt_two",
      "black_dst",
      "core_dst")
    for (condition <- conditions.children) {
      val dim_type = compact(condition \ "dim_type")
      val new_prefix = s"$prefix-$dim_type"
      for (k <- d) {
        val key = s"$new_prefix-$k"
        val v = compact(condition \ k)
        res += (key -> v)
      }
    }

  }

  val dictTypes: Map[String, (String, JValue, mutable.Map[String, String]) => Unit] = Map[String, (String, JValue, mutable.Map[String, String]) => Unit](
    "frequency_one_dim" -> process_frequency_one_dim,
    "frequency_count" -> process_frequency_count,
    "frequency_distinct" -> process_frequency_distinct,
    "black_list" -> process_black_list,
    "fp_exception" -> process_fp_exception,
    "grey_list" -> process_grey_list,
    "fuzzy_black_list" -> process_fuzzy_black_list,
    "geo_ip_distance" -> process_geo_ip_distance,
    "proxy_ip" -> process_proxy_ip,
    "gps_distance" -> process_gps_distance,
    "regex" -> process_regex,
    "event_time_diff" -> process_event_time_diff,
    "time_diff" -> process_time_diff,
    "active_days_two" -> process_active_days_two,
    "cross_event" -> process_cross_event,
    "cross_velocity_one_dim" -> process_cross_velocity_one_dim,
    "cross_velocity_count" -> process_cross_velocity_count,
    "cross_velocity_distinct" -> process_cross_velocity_distinct,
    "calculate" -> process_calculate,
    "last_match" -> process_last_match,
    "min_max" -> process_min_max,
    "count" -> process_count,
    "association_partner" -> process_association_partner,
    "discredit_count" -> process_discredit_count,
    "cross_partner" -> process_cross_partner,
    "four_calculation" -> process_four_calculation,
    "function_kit" -> process_function_kit,
    "usual_browser" -> process_usual_browser,
    "usual_device" -> process_usual_device,
    "usual_location" -> process_usual_location,
    "keyword" -> process_keyword,
    "android_cheat_app" -> process_android_cheat_app,
    "ios_cheat_app" -> process_ios_cheat_app,
    "android_emulator" -> process_android_emulator,
    "device_status_abnormal" -> process_device_status_abnormal,
    "suspected_team" -> process_suspected_team
  )



  def parseData(jsonContent: String,result:mutable.Map[String, String]): Unit = {
    val json = parse(jsonContent)
    val reason_code = compact(json \ "reason_code").replaceAll("\"","")
    if (reason_code != "200")
      return
    val rules = json \\ "rules"
    for (rule <- rules.children) {
      val rule_id = compact(rule \ "rule_id").replaceAll("\"","")
      val conditions = rule \\ "conditions"
      if (conditions.children.nonEmpty) {
        val rule_type = compact(conditions(0) \ "type").replaceAll("\"","")
        process_rule(rule_id, conditions, rule_type, result)
      }
    }

  }

  def process_rule(ruleId: String, conditions: JValue, ruleType: String, res: mutable.Map[String, String]): Unit = {
    val prefix = s"$ruleId-$ruleType"
    if (dictTypes.keySet.contains(ruleType)) {
      val method = dictTypes(ruleType)
      method(prefix, conditions, res)
    }
  }
  def main(args: Array[String]): Unit = {
    val datas=Source.fromFile("D:\\data\\test\\json\\univ_td.log")
    val container=mutable.Map[String,String]().empty
    for(line<- datas.getLines()){
      val verify=line.split("\t")(2).replaceAll("NULL","null")
      parseData(verify,container)
    }
    container.foreach(println(_))
    datas.close()
  }
}

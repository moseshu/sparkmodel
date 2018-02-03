package cn.com.google.demo

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.NodeBuffer

/**
  * Created by ThinkPad on 2017/8/21.
  */
object Title

class Document {
  private var useNextArgs: Any = _

  def setTitle(title: String): this.type = {
    println(title)
    this
  }

  def setAuthor(author: String): this.type = {
    println(author)
    this
  }

  def set(obj: Title.type): this.type = {
    useNextArgs = obj
    this
  }

  //  def to(arg:String)=if(useNextArgs==Title) ""=arg
}

class Book[T, U] extends Document {

  type Index = mutable.HashMap[String, (Int, Int)]

  def addChaper(chapter: String): Book[T, U] = {
    println("you add chapter:" + chapter)
    this
  }
}

case class Goods(date: String, price: Double, id: String)

case class People(name: String, age: Int)

object ScalaDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    /*val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()*/
    //    testDataSet(spark)
    //    testML(spark)
    //    testXuedai(spark)
    //    testDateDiff(spark)
//    testDemo(spark)
//    parseData(spark)
    /*val date=LocalDate.now()
    for(i <- 1 to 1){
      println(date.minusDays(i).toString)
    }*/
 val decim=DataTypes.createDecimalType()

  }


  def foo[A](x: A)(implicit f: A => String): Unit = {



  }

  def testDataSet(spark: SparkSession): Unit = {
    //    val data=spark.sparkContext.textFile("D:\\data\\test\\shiyanlou.txt").map(_.split(",")).filter(_.length==3).map(x=>Goods(x(0),x(1).toDouble,x(2))).toDF()
    //    data.createOrReplaceTempView("goods")
    //    spark.sql("select date,sum(price) agg from goods group by date").rdd.coalesce(1).saveAsTextFile("D:\\data\\test\\shiyanlou")
    val path = "`C:\\Users\\ThinkPad\\Desktop\\spark-master\\examples\\src\\main\\resources\\users.parquet`"
    //val df = spark.sql(s"select * from parquet.$path")
    /* val peopledf=spark.sparkContext.textFile("C:\\Users\\ThinkPad\\Desktop\\spark-master\\examples\\src\\main\\resources\\people.txt").map(_.split(",")).map{row=>
       People(row(0),row(1).toInt)
     }.toDF()*/
    //    peopledf.createOrReplaceTempView("people")
    spark.sparkContext.textFile("C:\\Users\\ThinkPad\\Desktop\\weibo.log").take(10).foreach(println(_))


  }

  def testXML(): Unit = {
    val items = new NodeBuffer
    items += <li>Fred</li>
    items += <li>Wilma</li>

  }

  def testMap(): Unit = {
    val map = Map(1 -> "h", 12 -> "b", 3 -> "c", 10 -> "d")
    val sort = map.min
    println(sort)
  }

  def testML(spark: SparkSession): Unit = {
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val wordsData = tokenizer.transform(sentenceData)

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(wordsData)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show()
  }

  def testXuedai(spark: SparkSession): Unit = {
    val except_xudai = spark.read.option("header", "true").option("inferschema", "true").option("mode", "DROPMALFORMED").csv("D:\\data\\test\\except_xudai.csv")
    val xudai_resolved = spark.read.option("header", "true").option("inferschema", "true").option("mode", "DROPMALFORMED").csv("D:\\data\\test\\xudai_resolved.csv")
    val df = except_xudai.join(xudai_resolved, Seq("loan_id", "channel"), "left")
    df.coalesce(1).write.format("csv").save("D:\\data\\1")

  }

  def testDateDiff(spark: SparkSession): Unit = {
    val schema = StructType(Array(StructField("start", StringType, nullable = true), StructField("end", StringType, nullable = true)))
    val rdd = spark.sparkContext.parallelize("2017-09-01 12:20:10,2017-09-03 14:39:20".split(",")).map(filed => Row(filed(0), filed(1)))
    spark.createDataFrame(rdd, schema).createOrReplaceTempView("test")
    val sql =
      """
        |select datediff(end,start) days from test
      """.stripMargin
    spark.sql(sql).show()

  }

  def testDemo(spark: SparkSession): Unit = {
    val schema = StructType(Array(StructField("id", DataTypes.StringType, nullable = true), StructField("mobile", DataTypes.StringType, nullable = true)))
    val rdd = spark.sparkContext.parallelize(Array("10002 13436969806", "10004 13753732474", "10005 15001257893")).map { x =>
      val field = x.split(" ")
      Row(field(0), field(1))
    }
    val rdd2 = spark.sparkContext.parallelize(Array("10003 13146616816", "10004 13753732475", "10007 15001257893")).map { x =>
      val field = x.split(" ")

      Row(field(0), field(1))
    }
    import spark.implicits._
    val df1=spark.createDataFrame(rdd, schema)
    val df2=spark.createDataFrame(rdd2, schema)
    val pt = df1.union(df2)
    df1.createOrReplaceTempView("user1")
    df2.createOrReplaceTempView("user2")
    val sqlTest=
      s"""
         |select a.id,a.mobile,b.id bd from user1 a left join user2 b on
         | (a.id=b.id or a.mobile=b.mobile) and a.id='10005'
       """.stripMargin
    spark.sql(sqlTest).show()
    val dd=df1.join(df2,df1("id")===df2("id")).select(df1("id"))
//   dd.show()
    pt.createOrReplaceTempView("user")
    val sql =
      s"""
         |select mobile,count(1) co from user group by mobile
      """.stripMargin
    val res = spark.sql(sql).join(pt, Seq("mobile")).select("id", "co")
    //    res.show()
    val aa = res.map { row =>
      val id = row.getAs[String]("id")
      val count = row.getAs[Long]("co")
      val res = id match {
        case "10004" | "10005" => (id, count)
        case "10002" => ("a", count)
        case _ => ("b", count)

      }
      res
    }
    println("scheme==================")
    aa.schema

//    aa.select(aa("_1")).show()
    aa.createOrReplaceTempView("temp")
    val sqltemp =
      s"""
         |select * from temp
       """.stripMargin
    println("===============================")
//    spark.sql(sqltemp).show()

    //    val rddtmp = spark.sparkContext.parallelize(Array(Row(null, null)))
    //    val schematmp = StructType(Array(StructField("id", StringType, true), StructField("co", LongType, true)))

    //res.show()
    //res.union(aa).show()
    //    res.coalesce(1).write.option("header", "true").option("sep","#").csv("D:\\data\\1")
    //    val c=spark.read.option("header", "true").option("sep","#").csv("D:/data/1/*")
    //    c.selectExpr("id").show()
    //    c.printSchema()

  }

  def array2String(array: Array[Map[String, String]]): String = {
    array.mkString(",")
  }

  def parseData(spark: SparkSession): Unit = {
    //selectExpr("userid_loanid_ds","overdue_day","create_time",
    //    "contacts_class1_blacklist_cnt","contacts_class2_blacklist_cnt",
    //    "smstotal","call_out_time","contact_morning_total","contacts_router_ratio","details")
    val data = spark.read.option("header", "true").option("sep", "#").csv("d:/wholejxl.csv")
    import spark.implicits._

    data.map { row =>
      val userid_loanid_ds = row.getAs[String]("userid_loanid_ds")
//      val overdue_day = row.getAs[String]("overdue_day")
      val contacts_class1_blacklist_cnt = row.getAs[String]("contacts_class1_blacklist_cnt")
      val contacts_class2_blacklist_cnt = row.getAs[String]("contacts_class2_blacklist_cnt")
      val smstotal = row.getAs[String]("smstotal")
      val call_out_time = row.getAs[String]("call_out_time")
      val contact_morning_total = row.getAs[String]("contact_morning_total")
      val contacts_router_ratio = row.getAs[String]("contacts_router_ratio")
      val details = row.getAs[String]("details")
//      val create_time=row.getAs[String]("create_time")
      val aa = details.substring(1, details.length - 1).replaceAll("'","")
      val array = aa.split(",")
      val cache = ArrayBuffer.empty[String]
      for (field <- array) {
        val a = field.split(":")
        var n2=""" "" """.trim
        val n1 = s""" "${a(0)}" """.trim
        if(a.length==2)n2=s""" "${a(1)}" """.trim
        cache +=(n1+":"+n2)
      }
      val result="{"+cache.mkString(",")+"}"
      (userid_loanid_ds,contacts_class1_blacklist_cnt,contacts_class2_blacklist_cnt,smstotal,call_out_time,contact_morning_total,contacts_router_ratio,result)
    }.selectExpr("_1 as userid_loanid_ds",
      "_2 as contacts_class1_blacklist_cnt","_3 as contacts_class2_blacklist_cnt","_4 as smstotal",
      "_5 as call_out_time","_6 as contact_morning_total","_7 as contacts_router_ratio","_8 as result"
    ).repartition(1).write.option("header", "true").option("sep", "#").csv("D:\\data\\1")

  }


}

case class CallDetails(id: String, count: Long)

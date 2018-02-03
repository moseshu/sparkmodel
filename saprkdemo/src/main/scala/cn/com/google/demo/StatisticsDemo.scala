package cn.com.google.demo

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import breeze.optimize.LBFGS
import cn.com.google.demo.StatisticsDemo.Algorithm.Algorithm
import cn.com.google.demo.StatisticsDemo.RegType.RegType
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.ann.{FeedForwardModel, FeedForwardTopology, FeedForwardTrainer}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, NaiveBayes, SVMWithSGD}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{L1Updater, SquaredL2Updater}
import org.apache.spark.mllib.regression.{IsotonicRegression, LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random
import scala.xml.NodeBuffer

/**
  * Created by ThinkPad on 2017/8/16.
  */
object StatisticsDemo {
  def main(args: Array[String]): Unit = {
        Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    val spark = SparkSession.builder().appName("Linear").master("local[*]").
      config("spark.sql.streaming.checkpointLocation", "d:/tmp/spark/spark-checkpoint")
      .getOrCreate()


    //    testLinear(spark,data_path)
    //    testLogistic(spark,data_path)
    //    testLogisticRegression(spark, data_path)
    //    testIsotonicRegression(spark, data_path)
    //    testNaviBayes(spark,data_path)
    //      testGroupBy(spark.sparkContext)
    //    testSort(spark)
    //    testBloomFilter(spark)
    val pram = Params("D:\\data\\test\\data\\mllib\\sample_binary_classification_data.txt")
    //     run(pram,spark.sparkContext)
    //    testDecisionTree(spark.sparkContext)
    testSqlStreaming(spark)
//    scoketClient()
  }

  def scoketClient(): Unit = {



    val client=new Socket("localhost",9999)
    val input=new BufferedReader(new InputStreamReader(client.getInputStream))
    var line=input.readLine()
    while (line!=null){
      println(line)
      line=input.readLine()
    }
    if(input!=null)
      input.close()
    if(client!=null)
      client.close()
  }


  def testSqlStreaming(spark: SparkSession): Unit = {
    import org.apache.spark.sql.functions._
    import spark.implicits._


    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", "10.103.51.140")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))
    println(words.printSchema())
    words.show()
    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  object Algorithm extends Enumeration {
    type Algorithm = Value
    val SVM, LR = Value
  }

  object RegType extends Enumeration {
    type RegType = Value
    val L1, L2 = Value
  }
  case class Raw(node: String, service: String, metric: Double)
  case class Params(
                     input: String = null,
                     numIterations: Int = 100,
                     stepSize: Double = 1.0,
                     algorithm: Algorithm = Algorithm.LR,
                     regType: RegType = RegType.L2,
                     regParam: Double = 0.01) extends AbstractParams[Params]

  def run(params: Params, sc: SparkContext): Unit = {


    Logger.getRootLogger.setLevel(Level.WARN)

    val examples = MLUtils.loadLibSVMFile(sc, params.input).cache()

    val splits = examples.randomSplit(Array(0.8, 0.2))
    val training = splits(0).cache()
    val test = splits(1).cache()

    val numTraining = training.count()
    val numTest = test.count()
    println(s"Training: $numTraining, test: $numTest.")

    examples.unpersist(blocking = false)

    val updater = params.regType match {
      case RegType.L1 => new L1Updater()
      case RegType.L2 => new SquaredL2Updater()
    }

    val model = params.algorithm match {
      case Algorithm.LR =>
        val algorithm = new LogisticRegressionWithLBFGS()
        algorithm.optimizer
          .setNumIterations(params.numIterations)
          .setUpdater(updater)
          .setRegParam(params.regParam)
        algorithm.run(training).clearThreshold()
      case Algorithm.SVM =>
        val algorithm = new SVMWithSGD()
        algorithm.optimizer
          .setNumIterations(params.numIterations)
          .setStepSize(params.stepSize)
          .setUpdater(updater)
          .setRegParam(params.regParam)
        algorithm.run(training).clearThreshold()
    }

    val prediction = model.predict(test.map(_.features))
    val predictionAndLabel = prediction.zip(test.map(_.label))

    val metrics = new BinaryClassificationMetrics(predictionAndLabel)

    println(s"Test areaUnderPR = ${metrics.areaUnderPR()}.")
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    sc.stop()
  }

  def testDecisionTree(sc: SparkContext): Unit = {
    val data = MLUtils.loadLibSVMFile(sc, "d:/data/test/data/mllib/sample_libsvm_data.txt")
    // Split the data into training and test sets (30% held out for testing)
    val splits = data.randomSplit(Array(0.7, 0.3))
    val (trainingData, testData) = (splits(0), splits(1))

    // Train a DecisionTree model.
    //  Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    // Evaluate model on test instances and compute test error
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count().toDouble / testData.count()
    println(s"Test Error = $testErr")
    println(s"Learned classification tree model:\n ${model.toDebugString}")

    // Save and load model
    //    model.save(sc, "target/tmp/myDecisionTreeClassificationModel")
    //    val sameModel = DecisionTreeModel.load(sc, "target/tmp/myDecisionTreeClassificationModel")
    // $example off$
    println(model.toString())
    sc.stop()
  }


  def testLogistic(spark: SparkSession, data_path: String): Unit = {
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, data_path + "\\sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.6, 0.4), 11L)
    val training = splits(0).cache()
    val test = splits(1)
    val model = new LogisticRegressionWithLBFGS().setNumClasses(10).run(training)
    val pridectionAndLablels = test.map {
      case LabeledPoint(lable, features) =>
        val prediction = model.predict(features)
        (prediction, lable)
    }
    val print_predict = pridectionAndLablels.take(20)
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }
    val metrics = new MulticlassMetrics(pridectionAndLablels)
    val pression = metrics.accuracy
    println("pression=" + pression)
  }

  def testLinear(spark: SparkSession, data_path: String): Unit = {

    val data = spark.sparkContext.textFile(data_path + "lpsa.data")
    val example = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }

    val numberExamples = example.count()
    val numInterations = 100
    val stepSize = 1
    val miniBatchFraction = 1.0
    val model = LinearRegressionWithSGD.train(example, numInterations, stepSize, miniBatchFraction)
    val prediction = model.predict(example.map(_.features))
    val predictionAndLabel = prediction.zip(example.map(_.label))
    val print_predict = predictionAndLabel.take(50)
    println("prediction" + "\t" + "lable")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    val loss = predictionAndLabel.map {
      case (p, v) =>
        val err = p - v
        err * err
    }.reduce(_ + _)
    val rmse = math.sqrt(loss / numberExamples)
    println(s"Test RMSE=$rmse")

  }

  def testStatiscs(spark: SparkSession, data_path: String): Unit = {
    val data = spark.sparkContext.textFile(data_path + "sample_stat.txt").map(_.split("\t")).map(f => f.map(x => x.toDouble))
    val data1 = data.map(f => Vectors.dense(f))
    val stat1 = Statistics.colStats(data1)
    /*println("max="+stat1.max)
    println("************************")
    println("min="+stat1.min)
    println("mean="+stat1.mean)
    println("variance="+stat1.variance)
    println("norml1="+stat1.normL1)
    println("norml2="+stat1.normL2)*/
    //println(data1.collect())
    val corr1 = Statistics.corr(data1, "pearson")
    val corr2 = Statistics.corr(data1, "spearman")
    val x1 = spark.sparkContext.parallelize(Array(1.0, 2.0, 3.0, 4.0))
    val y1 = spark.sparkContext.parallelize(Array(5.0, 6.0, 6.0, 6.0))
    val corr3 = Statistics.corr(x1, y1, "pearson")

  }

  def testLogisticRegression(spark: SparkSession, data_path: String): Unit = {
    val data = MLUtils.loadLibSVMFile(spark.sparkContext, data_path + "sample_libsvm_data.txt")
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(10)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    println(s"Accuracy = $accuracy")

    spark.stop()
  }

  def testIsotonicRegression(spark: SparkSession, data_path: String): Unit = {
    val data = spark.sparkContext.textFile(data_path + "sample_isotonic_regression_data.txt")
    val parseddata = data.map { line =>
      val parts = line.split(",").map(_.toDouble)
      (parts(0), parts(1), 1.0)
    }
    val splits = parseddata.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = new IsotonicRegression().setIsotonic(true).run(training)
    val x = model.boundaries
    val y = model.predictions
    println("boundaries" + "\t" + "preditions")
    for (i <- 0 to x.length - 1) println(x(i) + "\t" + y(i))
    val predictionAndLabel = test.map(point => {
      val predictionLabel = model.predict(point._2)
      (predictionLabel, point._1)
    })
    val predict = predictionAndLabel.take(30)
    for (i <- 0 to predict.length - 1) println(predict(i)._1 + "\t" + predict(i)._2)
    spark.stop()
  }

  def testNaviBayes(spark: SparkSession, path: String): Unit = {
    val data = spark.sparkContext.textFile(path + "sample_naive_bayes_data.txt")
    val parseData = data.map { line =>
      val parts = line.split(",")
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(" ").map(_.toDouble)))
    }
    val splits = parseData.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0)
    val test = splits(1)
    val model = NaiveBayes.train(training, lambda = 1.0, modelType = "multinomial")
    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))
    val printlabel = predictionAndLabel.take(20)
    for (i <- 0 to printlabel.length - 1) println(printlabel(i)._1 + "\t" + printlabel(i)._2)

  }

  def testGroupBy(sc: SparkContext): Unit = {
    var numMappers = 100
    var numKVPairs = 10000
    var valSize = 1000
    var numReducers = 36
    val pairs1 = sc.parallelize(0 until numMappers, numMappers).flatMap { p =>
      val ranGen = new Random
      var arr1 = new Array[(Int, Array[Byte])](numKVPairs)
      for (i <- 0 until numKVPairs) {
        val byteArr = new Array[Byte](valSize)
        ranGen.nextBytes(byteArr)
        arr1(i) = (ranGen.nextInt(Int.MaxValue), byteArr)
      }
      arr1
    }.cache
    // Enforce that everything has been calculated and in cache
    pairs1.count

    println(pairs1.groupByKey(numReducers).count)

    sc.stop()
  }

  def testSort(spark: SparkSession): Unit = {
    val unsort = spark.sparkContext.parallelize(List((2, 1), (3, 1), (10, 3), (4, 10)))
    unsort.sortBy(x => x._1, false).collect().foreach(println(_))
  }

  def testBloomFilter(spark: SparkSession): Unit = {
    val df = spark.createDataFrame(List(Person("moses", 25, "101"), Person("moses", 25, "101")
      , Person("moses", 25, "101"), Person("anne", 20, "102"), Person("anne", 20, "102"),
      Person("tiger", 35, "103"), Person("cat", 25, "104")))
    df.createOrReplaceTempView("person")
    val sql =
      s"""
         |select * from person
       """.stripMargin
    val df2 = spark.sql(sql)
    df2.distinct()
    //    df2.show()
    val filter = df.stat.bloomFilter(df("id"), 20, 0.01)
    val bc = spark.sparkContext.broadcast(filter)
    filter
  }
}

import scala.reflect.runtime.universe._

/**
  * Abstract class for parameter case classes.
  * This overrides the [[toString]] method to print all case class fields by name and value.
  *
  * @tparam T Concrete parameter class.
  */
abstract class AbstractParams[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  /**
    * Finds all case class fields in concrete class instance, and outputs them in JSON-style format:
    * {
    * [field name]:\t[field value]\n
    * [field name]:\t[field value]\n
    * ...
    * }
    */
  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"  $paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}

case class Person(name: String, age: Int, id: String)
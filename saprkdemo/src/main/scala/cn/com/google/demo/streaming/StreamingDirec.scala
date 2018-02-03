package cn.com.google.demo.streaming

/**
  * Created by ThinkPad on 2017/10/10.
  */
object StreamingDirec {
  /**
    * import kafka.common.TopicAndPartition
　　import kafka.message.MessageAndMetadata
　　import kafka.serializer.StringDecoder
　　import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
　　import org.I0Itec.zkclient.ZkClient
　　import org.apache.spark.streaming.{Seconds, StreamingContext}
　　import org.apache.spark.streaming.dstream.InputDStream
　　import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}

　　 val conf: Conf = new config.Conf("test-util.conf")
    val zkHost = conf.getString("kafka.zookeeper.connect")
    val brokerList=conf.getString("kafka.metadata.broker.list")
    val zkClient = new ZkClient(zkHost)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokerList,
      "zookeeper.connect" -> zkHost,
      "group.id" -> "testid")


    var kafkaStream: InputDStream[(String, String)] = null
    var offsetRanges = Array[OffsetRange]()
    val sc=SparkUtil.createSparkContext("test")
    val ssc=new StreamingContext(sc,Seconds(5))
    val topic="TEST_TOPIC"
    val topicDirs = new ZKGroupTopicDirs("TEST_TOPIC_spark_streaming_testid", topic)  //创建一个 ZKGroupTopicDirs 对象，对保存


    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}")     //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）

    var fromOffsets: Map[TopicAndPartition, Long] = Map()   //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    if (children > 0) {   //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
      for (i <- 0 until children) {
        val partitionOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}")
        val tp = TopicAndPartition(topic, i)
        fromOffsets += (tp -> partitionOffset.toLong)  //将不同 partition 对应的 offset 增加到 fromOffsets 中
      }

      val messageHandler = (mmd : MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())  //这个会将 kafka 的消息进行 transform，最终 kafak 的数据都会变成 (topic_name, message) 这样的 tuple
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    }
    else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("TEST_TOPIC")) //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
    }


    kafkaStream.transform{rdd=>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
      rdd
    }.map(_._2).foreachRDD(rdd=>{
      for (o <- offsetRanges) {
        val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)  //将该 partition 的 offset 保存到 zookeeper
      }
      rdd.foreach(s=>println(s))
    })

    ssc.start()
    ssc.awaitTermination()
    */
}

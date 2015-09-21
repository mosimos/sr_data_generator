import spray.json._
import DefaultJsonProtocol._

import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

object Reasoner {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Reasoner")
    val ssc = new StreamingContext(conf, Seconds(1))

    //val triplestream = ssc.socketTextStream("localhost", 9999)
    val topicMap = "gtfs".split(",").map((_, 1)).toMap

    val messages = KafkaUtils.createStream(ssc, args(0), "mygroup", topicMap)
    
    //parse triples from JSON
    val triplestream = messages.map(_._2)
    val json_asts = triplestream.map(_.parseJson)
    val triple_objects = json_asts.map(_.convertTo[Array[String]])

    //work with triples

    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val arrived = windowed.filter(_(1).contains("hasArrived"))

    val arr_stts = arrived.map(x => (x(0),x(2)))

    arr_stts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


import spray.json._
import DefaultJsonProtocol._

import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.DStream

import scala.annotation.switch

object Reasoner {
  def main(args: Array[String]) {

    var query = args(0).toInt

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

    (query: @switch) match {
      case 1 => query01(triple_objects)
      case 2 => query02(triple_objects)
      case 3 => query03(triple_objects)
      case 4 => query04(triple_objects)
      case 5 => query05(triple_objects)
      case 6 => query06(triple_objects)
      case 7 => query07(triple_objects)
      case 8 => query08(triple_objects)
      case 9 => query09(triple_objects)
      case 10 => query10(triple_objects)
      case 11 => query11(triple_objects)
      case 12 => query12(triple_objects)
      case _ => println("unexpected query number")
    }

    ssc.start()
    ssc.awaitTermination()
  }

  def query01(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val arrived = windowed.filter(_(1).contains("hasArrived"))

    val result = arrived.map(x => (x(0), x(2)))

    result.print()
  }

  def query02(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val arrived = windowed.filter(_(1).contains("hasArrived"))
    val arrived_30 = windowed.filter(_(2).toInt > 30)

    val result = arrived_30.map(x => (x(0), x(2)))

    result.print()
  }

  def query03(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay"))
    val arrived = windowed.filter(_(1).contains("hasArrived"))
    val both = delayed.union(arrived)

    val result = both.map(x => (x(0), x(2)))

    result.print()
  }

  def query04(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay")).map(x => (x(0), x(2)))
    val arrived = windowed.filter(_(1).contains("hasArrived")).map(x => (x(0), x(2)))
    val joined = delayed.leftOuterJoin(arrived)

    joined.print()
  }

  def query05(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay"))

    val result = delayed.map(x => (x(0), x(2).toInt / 60))

    result.print()
  }

  def query06(triple_objects: DStream[Array[String]]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay"))
    val result = delayed.map(x => (x(0), x(2).toInt / 60))

    result.print()
  }

  def query07(triple_objects: DStream[Array[String]]) {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val stt = delayed.map(x => x(0))
    val count = stt.countByWindow(Seconds(1), Seconds(1))
    //TODO check if this is right
    //Docu says: Return a new DStream in which each RDD has a single element generated by counting the number of elements in a sliding window over this DStream.

    count.print()
  }

  def query08(triple_objects: DStream[Array[String]]) {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val stt = delayed.map(x => x(0))
    val count = stt.countByValueAndWindow(Seconds(1), Seconds(1))
    //TODO check if this is right
    //Docu says: Return a new DStream in which each RDD contains the count of distinct elements in RDDs in a sliding window over this DStream.
    
    count.print()
  }

  def query09(triple_objects: DStream[Array[String]]) {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val delays = delayed.map(x => x(2))
    val max = delays.reduceByWindow((a, b) => Math.max(a.toInt, b.toInt).toString, Seconds(1), Seconds(1))
    
    max.print()
  }

  def query10(triple_objects: DStream[Array[String]]) {
    //TODO not sure if we can order, research further
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay"))

    val result = delayed.map(x => (x(0), x(2)))

    result.print()
  }

  def query11(triple_objects: DStream[Array[String]]) {
    //TODO implement static data
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay")).map(x => (x(0), x(2)))

    delayed.print()
  }

  def query12(triple_objects: DStream[Array[String]]) {
    //TODO implement static data
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay")).map(x => (x(0), x(2)))

    delayed.print()
  }


}


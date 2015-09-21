import spray.json._
import DefaultJsonProtocol._

import org.apache.spark._
import org.apache.spark.streaming._

object Reasoner {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Reasoner")
    val ssc = new StreamingContext(conf, Seconds(1))

    val triplestream = ssc.socketTextStream("localhost", 9999)
    
    //parse triples from JSON
    val json_asts = triplestream.map(_.parseJson)
    val triple_objects = json_asts.map(_.convertTo[Array[String]])

    //work with triples

    val arrived = triple_objects.filter(_(1).contains("hasArrived"))

    val arr_stts = arrived.map(_(0))

    arr_stts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


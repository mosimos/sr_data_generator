import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query12 extends Query {
  def process(triple_objects: DStream[Seq[String]], static_data: RDD[(String, String)]) : DStream[Seq[String]] = {
    //TODO test this
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay")).map(x => (x(0), x(2)))
    val joined = delayed.transform(rdd => rdd.leftOuterJoin(static_data))
    val result = joined.map(x => Seq(x._1, x._2._1, (x._2._2).toString))
    return result
  }
}


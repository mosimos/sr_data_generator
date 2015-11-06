import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query01 extends Query {
  def process(triple_objects: DStream[Seq[String]], static_data: RDD[(String, String)]) : DStream[Seq[String]] = {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val arrived = windowed.filter(_(1).contains("hasArrived"))

    val result = arrived.map(x => Seq(x(0), x(2)))
    return result
  }
}


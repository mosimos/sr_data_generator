import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query03 extends Query {
  def process(triple_objects: DStream[Seq[String]], static_data: RDD[(String, String)]) : DStream[Seq[String]] = {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay"))
    val arrived = windowed.filter(_(1).contains("hasArrived"))
    val both = delayed.union(arrived)

    val result = both.map(x => Seq(x(0), x(2)))
    return result
  }
}


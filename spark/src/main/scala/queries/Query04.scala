import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query04 extends Query {
  def process(triple_objects: DStream[Array[String]], static_data: RDD[(String, String)]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val delayed = windowed.filter(_(1).contains("hasDelay")).map(x => (x(0), x(2)))
    val arrived = windowed.filter(_(1).contains("hasArrived")).map(x => (x(0), x(2)))
    val joined = delayed.leftOuterJoin(arrived)

    joined.print()
  }
}

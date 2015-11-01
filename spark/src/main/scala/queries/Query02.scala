import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query02 extends Query {
  def process(triple_objects: DStream[Array[String]], static_data: RDD[(String, String)]) {
    val windowed = triple_objects.window(Seconds(1), Seconds(1))
    val arrived = windowed.filter(_(1).contains("hasArrived"))
    val arrived_30 = windowed.filter(_(2).toInt > 30)

    val result = arrived_30.map(x => (x(0), x(2)))

    result.print()
  }
}


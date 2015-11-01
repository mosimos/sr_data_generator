import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query09 extends Query {
  def process(triple_objects: DStream[Array[String]], static_data: RDD[(String, String)]) {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val delays = delayed.map(x => x(2))
    val max = delays.reduceByWindow((a, b) => Math.max(a.toInt, b.toInt).toString, Seconds(1), Seconds(1))
    
    max.print()
  }
}


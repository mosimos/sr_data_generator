import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query08 extends Query {
  def process(triple_objects: DStream[Array[String]], static_data: RDD[(String, String)]) {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val stt = delayed.map(x => x(0))
    val count = stt.countByValueAndWindow(Seconds(1), Seconds(1))
    //TODO check if this is right
    //Docu says: Return a new DStream in which each RDD contains the count of distinct elements in RDDs in a sliding window over this DStream.
    
    count.print()
  }
}


import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

trait Query {
  def process(triple_objects: DStream[Array[String]], static_data: RDD[(String, String)])
}


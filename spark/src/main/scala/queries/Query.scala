import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

trait Query {
  def process(triple_objects: DStream[Seq[String]], static_data: RDD[(String, String)]) : DStream[Seq[String]]
}


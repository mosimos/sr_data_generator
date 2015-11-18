/*
 *   Copyright 2015 Andreas Mosburger
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import spray.json._
import DefaultJsonProtocol._

import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD

import java.lang.ClassLoader
import java.lang.ClassNotFoundException

/*
 ../../spark-1.5.0-bin-hadoop2.6/bin/spark-submit --class "Reasoner" --master 'local[4]' target/scala-2.10/gtfs-reasoner-assembly.jar localhost Query01 ../../gtfs_datasets/portland/stop_times.txt ../../results/spark/
 */

object Reasoner {
  def main(args: Array[String]) {

    if (args.length != 3 && args.length !=4) {
      println("error: wrong number of arguments")
      println("usage: Reasoner zk_quorum query output_dir [static_dataset]")
      println()
      println("propositional arguments:");
      println("  zk_quorum\t\tZookeeper quorum, where to listen for streaming data using Kafka");
      println("  query\t\t\tclass name of query to run");
      println("  output_dir\t\tdirectory where to output data");
      println();
      println("optional arguments:");
      println("  static_dataset\tfile containing a static dataset");
      System.exit(1)
    }

    var query = args(1)

    val conf = new SparkConf().setAppName("Reasoner")
    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("~/spark_chkpointdir")
    var static_data = ssc.sparkContext.parallelize(Array((" ", " ")))
    if (args.length == 4) {
      //load static data
      val input_file = ssc.sparkContext.textFile(args(3))
      //we only need trip_id, stop_id and stop_sequence for our simple examples
      val split = input_file.map(x => x.split(','))
      static_data = split.map(x => ("stoptime" + x(0) + x(4), x(3)))

      println()
      println()
      println(static_data.count() + "stts read in ===========================")
      println()
      println()
    }

    //val triplestream = ssc.socketTextStream("localhost", 9999)
    val topicMap = "gtfs".split(",").map((_, 1)).toMap

    val messages = KafkaUtils.createStream(ssc, args(0), "mygroup", topicMap)

    //parse triples from JSON
    val triplestream = messages.map(_._2)
    val json_asts = triplestream.map(_.parseJson)
    val triple_objects = json_asts.map(_.convertTo[Seq[String]])

    //work with triples
    
    try {
      var loader = Reasoner.getClass().getClassLoader()
      var query_class = loader.loadClass(query)
      var q = query_class.newInstance

      var res = q match {
        case q1: Query => q1.process(triple_objects, static_data)
        case _ => null
      }

      if (res == null) {
        println("error: couldn't load query " + query)
      }
      else {
        res.print()
        res.saveAsTextFiles(args(2) + query)

        ssc.start()
        ssc.awaitTermination()
      }
    } catch {
      case e: ClassNotFoundException => println("error: couldn't find query " + query)
    }

  }

}


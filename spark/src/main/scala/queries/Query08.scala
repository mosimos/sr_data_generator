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

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream

class Query08 extends Query {
  def process(triple_objects: DStream[Seq[String]], static_data: RDD[(String, String)]) : DStream[Seq[String]] = {
    val delayed = triple_objects.filter(_(1).contains("hasDelay"))
    val stt = delayed.map(x => x(0))
    val count = stt.countByValueAndWindow(Seconds(1), Seconds(1))
    //TODO check if this is right
    //Docu says: Return a new DStream in which each RDD contains the count of distinct elements in RDDs in a sliding window over this DStream.
    return count.map(x => Seq(x._1, (x._2).toString))
  }
}


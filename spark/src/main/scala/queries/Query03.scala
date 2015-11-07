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


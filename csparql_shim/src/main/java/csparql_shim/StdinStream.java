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

package csparql_shim;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

/**
 * reads RDF triples from stdin and streams them to csparql
 * a triple is expected as an array in JSON
 */
public class StdinStream extends RdfStream implements Runnable
{
	boolean stop = false;
	long sleep = 1;

	/** The logger. */
	protected final Logger logger = LoggerFactory.getLogger(StdinStream.class);	


	/**
	 * @param context
	 * @param uri
	 */
	public StdinStream(final String iri) {
		super(iri);
	}

	public void stop() {
		stop = true;
	}

	/**
	 * @param rate
	 */
	public void setRate(int rate) {
		sleep = 1000 / rate;
	}

	/**
	 * start listening to stdin and forwarding to csparql
	 */
	@Override
	public void run() {
		try {
			JSONParser parser = new JSONParser();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			String line;

			System.out.println(getIRI());
			while ((line = reader.readLine()) != null && !stop) {
				try {
					Object obj = parser.parse(line);
					JSONArray array = (JSONArray) obj;

					//stream the triple
					final RdfQuadruple q = new RdfQuadruple((String) array.get(0), (String) array.get(1), (String) array.get(2), System.currentTimeMillis());
					//System.out.println((String) array.get(0) + " " + (String) array.get(1) + " " + (String) array.get(2));
					this.put(q);
				} catch (ParseException pe) {
					System.err.println("Error when parsing input, incorrect JSON.");
				}

				if (sleep > 0) {
					try {
						Thread.sleep(sleep);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}

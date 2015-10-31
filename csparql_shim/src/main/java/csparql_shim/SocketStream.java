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
import java.io.DataInputStream;

import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;

/**
 * reads RDF triples from a socket and streams them to csparql
 * a triple is expected as an array in JSON
 */
public class SocketStream extends RdfStream implements Runnable
{
	int port = 9999;
	boolean stop = false;

	/** The logger. */
	protected final Logger logger = LoggerFactory.getLogger(SocketStream.class);	


	/**
	 * @param context
	 * @param uri
	 */
	public SocketStream(final String iri, int port) {
		super(iri);
		this.port = port;
	}

	public void stop() {
		stop = true;
	}

	/**
	 * start listening on a socket and forwarding to csparql
	 * stream in defined windows to allow comparison with cqels
	 */
	@Override
	public void run() {
		ServerSocket ssock = null;
		Socket sock = null;
		try {
			ssock = new ServerSocket(this.port);
			sock = ssock.accept();

			DataInputStream is = new DataInputStream(sock.getInputStream());
			JSONParser parser = new JSONParser();

			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;

			int windowcount = 1;

			//loop for streaming in data
			while ((line = reader.readLine()) != null && !stop) {

				try {
					Object obj = parser.parse(line);
					JSONArray array = (JSONArray) obj;

					//stream the triple
					final RdfQuadruple q = new RdfQuadruple((String) array.get(0), (String) array.get(1), (String) array.get(2), System.currentTimeMillis());
					this.put(q);
					System.out.println("triple sent at: " + System.currentTimeMillis());
				} catch (ParseException pe) {
					System.err.println("Error when parsing input, incorrect JSON.");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}


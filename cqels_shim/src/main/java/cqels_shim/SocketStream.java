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

package cqels_shim;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.DataInputStream;

import java.net.ServerSocket;
import java.net.Socket;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import com.hp.hpl.jena.graph.Node;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * reads RDF triples from a socket and streams them to cqels
 * a triple is expected as an array in JSON
 */
public class SocketStream extends RDFStream implements Runnable
{
	int port = 9999;
	boolean stop = false;
	long sleep = 0;

	/**
	 * @param context
	 * @param uri
	 */
	public SocketStream(ExecContext context, String uri, int port) {
		super(context, uri);
		this.port = port;
	}

	@Override
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
	 * start listening on the socket and forwarding to cqels
	 */
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

			while ((line = reader.readLine()) != null && !stop) {
				try {
					Object obj = parser.parse(line);
					JSONArray array = (JSONArray) obj;

					//stream the triple
					stream(n((String) array.get(0)), n((String) array.get(1)), n((String) array.get(2)));
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

	public static  Node n(String st){
		return Node.createURI(st);
	}
}


package cqels_shim;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import com.hp.hpl.jena.graph.Node;

import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * reads RDF triples from stdin and streams them to cqels
 * a triple is expected as an array in JSON
 */
public class StdinStream extends RDFStream implements Runnable
{
	boolean stop = false;
	long sleep = 500;

	/**
	 * @param context
	 * @param uri
	 */
	public StdinStream(ExecContext context, String uri) {
		super(context, uri);
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
	 * start listening to stdin and forwarding to cqels
	 */
	public void run() {
		try {
			JSONParser parser = new JSONParser();
			BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
			String line;

			while ((line = reader.readLine()) != null && !stop) {
				Object obj = parser.parse(line);
				JSONArray array = (JSONArray) obj;

				//stream the triple
				//System.out.println((String) array.get(0) + " " + (String) array.get(1) + " " + (String) array.get(2));
				//System.out.println(n((String) array.get(0)) + " " + n((String) array.get(1)) + " " + n((String) array.get(2)));
				stream(n((String) array.get(0)), n((String) array.get(1)), n((String) array.get(2)));
				//System.out.println("triple streamed");

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
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	public static  Node n(String st){
		return Node.createURI(st);
	}
}


package cqels_shim;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.data.Mapping;
import com.hp.hpl.jena.sparql.core.Var;

import java.util.Iterator;

import cqels_shim.StdinStream;

/**
 * a minimal shim to provide a standardized interface to cqels
 */
public class CqelsShim 
{
	public static void main(String[] args) {
		String home = args[0];
		String path = args[1];
		String querypath = args[2];

		final ExecContext context = new ExecContext(home, false);

		//TODO figure out how loading this should work
		//context.loadDataset("http://deri.org/floorplan/", "../cqels_data/floorplan.rdf");
		context.loadDefaultDataset(path);

		//initialize stream
		StdinStream stream = new StdinStream(context, "http://deri.org/streams/rfid");

		//register query
		//TODO read query from file
		String queryString = "PREFIX lv: <http://deri.org/floorplan/> " +
			"PREFIX dc: <http://purl.org/dc/elements/1.1/> " +
			"PREFIX foaf: <http://xmlns.com/foaf/0.1/> " +
			"SELECT ?locName "  +
			"FROM NAMED <http://deri.org/floorplan/> " +
			"WHERE { " +
			"STREAM <http://deri.org/streams/rfid> [NOW] "  +
			"{?person lv:detectedAt ?loc} "  +
			//"{?person foaf:name \"AUTHORNAME\"^^<http://www.w3.org/2001/XMLSchema#string> } " +
			"GRAPH <http://deri.org/floorplan/> "  +
			"{?loc lv:name ?locName} " +
			"}";
		//String queryString = "PREFIX lv: <http://deri.org/floorplan/> SELECT  ?person1 ?person2 FROM NAMED <http://deri.org/floorplan/> WHERE { GRAPH <http://deri.org/floorplan/>  {?loc1 lv:connected ?loc2} STREAM <http://deri.org/streams/rfid> [NOW] {?person1 lv:detectedAt ?loc1} STREAM <http://deri.org/streams/rfid> [RANGE 3s] {?person2 lv:detectedAt ?loc2} }";

		ContinuousSelect selQuery = context.registerSelect(queryString);
		selQuery.register(new ContinuousListener() {
			public void update(Mapping mapping) {
				String result = "";
				for(Iterator<Var> vars = mapping.vars(); vars.hasNext(); ) {
					//Use context.engine().decode(...) to decode the encoded value to RDF Node
					result += " " + context.engine().decode(mapping.get(vars.next()));
				}
				System.out.println(result);
			} 
		});

		System.out.println("listening for data");

		//start streaming
		(new Thread(stream)).start();
		
		//TODO add way to exit nicely
	}
}


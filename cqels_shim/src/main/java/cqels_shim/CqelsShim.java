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

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.data.Mapping;
import com.hp.hpl.jena.sparql.core.Var;

import java.util.Iterator;
import java.util.List;
import java.lang.StringBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.io.IOException;


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

		Path qpath = Paths.get(querypath);
		List<String> lines;
		StringBuilder sb = new StringBuilder();

		try {
			lines = Files.readAllLines(qpath, StandardCharsets.UTF_8);

			for (String s : lines) {
				sb.append(s).append(" ");
			}
		} catch (IOException e) {
			System.exit(-1);
		}

		String queryString = sb.toString();

		//TODO needed?
		//queryString = queryString.replace('\n', '');
		
		//String queryString = "PREFIX ns1: <http://kr.tuwien.ac.at/dhsr/> " +
			//"SELECT ?stop ?name " +
			//"FROM NAMED <http://kr.tuwien.ac.at/dhsr/> " +
			//"WHERE { " +
			//"STREAM <http://kr.tuwien.ac.at/dhsr/stream> [NOW] " +
			//"{?stop ns1:hasName ?name} }";


		final ExecContext context = new ExecContext(home, false);

		context.loadDataset("http://kr.tuwien.ac.at/dhsr/", path);

		//initialize stream
		StdinStream stream = new StdinStream(context, "http://kr.tuwien.ac.at/dhsr/stream");

		//register query
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


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
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;


import cqels_shim.StdinStream;
import cqels_shim.SocketStream;

/**
 * a minimal shim to provide a standardized interface to cqels
 */
public class CqelsShim 
{
	public static void main(String[] args) {
		if (args.length != 4 && args.length != 5) {
			System.err.println("error: wrong number of arguments");
			System.err.println("usage: java -jar CqelsShim.jar cqels_home port queryfile outputfile [static_dataset]");
			System.exit(-1);
		}

		String home = args[0];
		int port = Integer.parseInt(args[1]);
		String querypath = args[2];

		Path qpath = Paths.get(querypath);
		List<String> lines;
		StringBuilder sb = new StringBuilder();

		try {
			lines = Files.readAllLines(qpath, StandardCharsets.UTF_8);

			for (String s : lines) {
				if (!s.startsWith("#")) {
					sb.append(s).append(" ");
				}
			}
		} catch (IOException e) {
			System.exit(-1);
		}

		String queryString = sb.toString();

		final ExecContext context = new ExecContext(home, false);

		if (args.length == 5) {
			context.loadDataset("http://kr.tuwien.ac.at/dhsr/", args[4]);
		}

		//initialize stream
		//StdinStream stream = new StdinStream(context, "http://kr.tuwien.ac.at/dhsr/stream");
		SocketStream stream = new SocketStream(context, "http://kr.tuwien.ac.at/dhsr/stream", port);

		//register query
		ContinuousSelect selQuery = context.registerSelect(queryString);


		try {
			final Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[3]), "utf-8"));
			selQuery.register(new ContinuousListener() {
				public void update(Mapping mapping) {
					String result = "";
					for(Iterator<Var> vars = mapping.vars(); vars.hasNext(); ) {
						//Use context.engine().decode(...) to decode the encoded value to RDF Node
						result += " " + context.engine().decode(mapping.get(vars.next()));
					}
					System.out.println(result);
					try {
						writer.write(result + "\n");
						writer.flush();
					} catch (IOException e) {
						System.err.println(e);
					}
				}
			});
		} catch (IOException ex) {
			System.err.println("error: couldn't open outputfile " + args[3]);
			System.exit(1);
		}

		System.out.println("listening for data");

		//start streaming
		(new Thread(stream)).start();
		
		//can't find a way to exit nicely, cqels continues even if we stop our thread
		//nowhere to call writer.close() ?
	}
}


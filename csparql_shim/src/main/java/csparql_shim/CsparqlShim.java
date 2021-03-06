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

import java.util.Iterator;
import java.util.List;
import java.lang.StringBuilder;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.text.ParseException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.larkc.csparql.core.engine.ConsoleFormatter;
import eu.larkc.csparql.core.engine.CsparqlEngine;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;

import csparql_shim.StdinStream;
import csparql_shim.SocketStream;
import csparql_shim.FileFormatter;

/**
 * a minimal shim to provide a standardized interface to csparql
 */
public class CsparqlShim
{
	private static Logger logger = LoggerFactory.getLogger(CsparqlShim.class);

	public static void main(String[] args) {
		try {
			PropertyConfigurator.configure(new URL("http://streamreasoning.org/configuration_files/csparql_readyToGoPack_log4j.properties"));
		} catch (MalformedURLException e) {
			logger.error(e.getMessage(), e);
		}

		if (args.length != 3 && args.length != 4) {
			System.err.println("error: wrong number of arguments");
			System.err.println("usage: java -jar CsparqlShim.java port queryfile outputfile [static_dataset]");
			System.err.println();
			System.err.println("Provides a standardized interface to the C-SPARQL engine.");
			System.err.println();
			System.err.println("propositional arguments:");
			System.err.println("  port\t\t\tport for listening for streaming data");
			System.err.println("  queryfile\t\tfile containing a query");
			System.err.println("  outputfile\t\toutput file");
			System.err.println();
			System.err.println("optional arguments:");
			System.err.println("  static_dataset\tfile containing a static dataset");
			System.exit(-1);
		}

		int port = Integer.parseInt(args[0]);
		String querypath = args[1];

		Path qpath = Paths.get(querypath);
		List<String> lines;
		StringBuilder sb = new StringBuilder();

		//read the query from the file given by querypath
		try {
			lines = Files.readAllLines(qpath, StandardCharsets.UTF_8);

			for (String s : lines) {
				//filter out comments
				if (!s.startsWith("#")) {
					sb.append(s).append(" ");
				}
			}
		} catch (IOException e) {
			System.exit(-1);
		}

		String queryString = sb.toString();
		//System.out.println(queryString);

		CsparqlEngine engine = new CsparqlEngineImpl();

		/*
		 * Choose one of the the following initialize methods: 
		 * 1 - initialize() - Inactive timestamp function - Inactive injecter 
		 * 2 - initialize(int* queueDimension) - Inactive timestamp function -
		 *     Active injecter with the specified queue dimension (if 
		 *     queueDimension = 0 the injecter will be inactive) 
		 * 3 - initialize(boolean performTimestampFunction) - if
		 *     performTimestampFunction = true, the timestamp function will be
		 *     activated - Inactive injecter 
		 * 4 - initialize(int queueDimension, boolean performTimestampFunction) - 
		 *     if performTimestampFunction = true, the timestamp function will
		 *     be activated - Active injecter with the specified queue dimension
		 *     (if queueDimension = 0 the injecter will be inactive)
		 */
		//TODO find out what this means
		engine.initialize(true);

		if (args.length == 4) {
			//load static data set
			try {
				byte[] encoded = Files.readAllBytes(Paths.get(args[3]));
				String content = new String(encoded, StandardCharsets.UTF_8);
				engine.putStaticNamedModel("http://kr.tuwien.ac.at/dhsr/", content);
			} catch (IOException e) {
				System.err.println("Couldn't load static data from file " + args[3]);
				System.err.println(e);
			}
		}

		//initialize stream
		//StdinStream stream = new StdinStream("http://kr.tuwien.ac.at/dhsr/stream");
		SocketStream stream = new SocketStream("http://kr.tuwien.ac.at/dhsr/stream", port);

		engine.registerStream(stream);

		//register query
		CsparqlQueryResultProxy c1 = null;

		try {
			c1 = engine.registerQuery(queryString, false);
			logger.debug("Query: {}", queryString);
			logger.debug("Query Start Time : {}", System.currentTimeMillis());
		} catch (final ParseException ex) {
			logger.error(ex.getMessage(), ex);
		}

		// Attach a Result Formatter to the query result proxy

		FileFormatter ff = new FileFormatter(args[2]);
		if (c1 != null) {
			c1.addObserver(new ConsoleFormatter());
			c1.addObserver(ff);
		}
		else {
			System.err.println("error, c1 == null");
		}


		System.out.println("listening for data");

		//start streaming
		(new Thread(stream)).start();
		
		try {
			System.in.read();
		} catch(IOException e) {
			System.err.println(e);
		}

		engine.unregisterQuery(c1.getId());
		engine.unregisterStream(stream.getIRI());
		ff.close();
	}
}


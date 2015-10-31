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

		if (args.length != 2 && args.length != 3) {
			System.out.println("error: wrong number of arguments");
			System.out.println("usage: java -jar CsqarlShim.java port queryfile [static_dataset]");
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

		if (args.length == 3) {
			//load static data set
			try {
				byte[] encoded = Files.readAllBytes(Paths.get(args[2]));
				String content = new String(encoded, StandardCharsets.UTF_8);
				engine.putStaticNamedModel("http://kr.tuwien.ac.at/dhsr/", content);
			} catch (IOException e) {
				System.out.println("Couldn't load static data from file " + args[2]);
				System.out.println(e);
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

		if (c1 != null) {
			c1.addObserver(new ConsoleFormatter());
		}
		else {
			System.out.println("error, c1 == null");
		}


		System.out.println("listening for data");

		//start streaming
		(new Thread(stream)).start();
		
		try {
			Thread.sleep(50000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//engine.unregisterQuery(c1.getId());
		//engine.unregisterStream(stream.getIRI());

		//TODO add way to exit nicely
	}
}


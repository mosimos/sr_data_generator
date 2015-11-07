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

import java.util.Observable;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.ResultFormatter;

import java.io.IOException;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;

public class FileFormatter extends ResultFormatter {
	private Writer writer;

	public FileFormatter(String file) {
		try {
			this.writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), "utf-8"));
		} catch (IOException ex) {
			System.err.println("error: couldn't open outputfile " + file);
		}
	}

	@Override
	public void update(Observable o, Object arg) {
		RDFTable q = (RDFTable) arg;

		try {
			//System.out.println("-------"+ q.size() + " results at SystemTime=["+System.currentTimeMillis()+"]--------");
			for (final RDFTuple t : q) {
				this.writer.write(t.toString() + "\n");
			}
			this.writer.flush();
		} catch (IOException ex) {
			System.err.println(ex);
		}
	}

	public void close() {
		try {
			this.writer.close();
		} catch (IOException ex) {
			System.err.println(ex);
		}
	}
}


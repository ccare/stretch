package com.metadatis.stretch.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Set;
import java.util.TreeSet;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class PredicatesCollector extends StreamingConverterBase {

	final Set<String> predicates = new TreeSet<String>();

	@Override
	public void convert(InputStream in, OutputStream out) throws IOException {
		PrintWriter wr = new PrintWriter(out);
		try {
			RDFParser p = Rio.createParser(RDFFormat.NTRIPLES);
			p.setRDFHandler(new RDFHandlerBase() {
				
					@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
						predicates.add(st.getPredicate().stringValue());
					}
				
			});
			p.parse(in, "http://tmp.metadatis.com");
			for (String pred : predicates) {
				wr.println(pred);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (wr != null) {
				wr.close();
			}
		}
	}

}

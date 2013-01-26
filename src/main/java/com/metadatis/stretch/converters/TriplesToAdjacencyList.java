package com.metadatis.stretch.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class TriplesToAdjacencyList extends StreamingConverterBase {
	
	private final Map<String, String> predicates;

	public TriplesToAdjacencyList(Map<String, String> predicates) {
		this.predicates = predicates;
	}

	@Override
	public void convert(InputStream in, OutputStream out) throws IOException {
		final PrintWriter wr = new PrintWriter(out);
		try {
			RDFParser p = Rio.createParser(RDFFormat.NTRIPLES);
			p.setRDFHandler(new RDFHandlerBase() {
				
				Resource currentSubject;
				Collection<Statement> statements = new LinkedList<Statement>();
				
				@Override
				public void handleStatement(Statement st) throws RDFHandlerException {
					if (currentSubject == null) {
						statements.add(st);
						currentSubject = st.getSubject();
					}
					else if (! currentSubject.equals(st.getSubject())) {
						write(wr);
						statements.clear();
						statements.add(st);
						currentSubject = st.getSubject();
					}
					else {
						statements.add(st);
					}
				}
				
				@Override
				public void endRDF() throws RDFHandlerException {
					write(wr);
				}

				private void write(final PrintWriter wr) {
					if (! statements.isEmpty()) {
						wr.print(currentSubject.stringValue());
						for (Statement s : statements) {
							wr.print('\t');
							Value object = s.getObject();
							if (object instanceof URI) {
								String pred = s.getPredicate().stringValue();
								wr.print('@');
								wr.print(predicates.get(pred));
								wr.print(' ');
								wr.print(object.stringValue());
							}							
						}
						wr.print('\n');
					}
				}
				
			});
			p.parse(in, "http://tmp.metadatis.com");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (wr != null) {
				wr.close();
			}
		}
	}

}

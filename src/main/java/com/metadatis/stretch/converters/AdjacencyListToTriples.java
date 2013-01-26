package com.metadatis.stretch.converters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Map;
import java.util.Scanner;

import org.openrdf.model.URI;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

public class AdjacencyListToTriples extends StreamingConverterBase {
	
	private final Map<String, String> predicates;

	public AdjacencyListToTriples(Map<String, String> predicates) {
		this.predicates = predicates;
	}

	@Override
	public void convert(InputStream in, OutputStream out) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		try {
			RDFWriter w = Rio.createWriter(RDFFormat.NTRIPLES, out);
			w.startRDF();
			while (reader.ready()) {
				String line = reader.readLine();
				Scanner sc = new Scanner(line);
				sc.useDelimiter("\t");
				String subjectStr = sc.next();
				URI s = new URIImpl(subjectStr);
				while (sc.hasNext()) {
					String next = sc.next();
					String[] split = next.split(" ");
					if (split.length < 2) {
						continue;
					}
					String pred = predicates.get(split[0]);
					URI p = new URIImpl(pred);
					URI o = new URIImpl(split[1]);
					w.handleStatement(new StatementImpl(s, p, o));
				}
				
			}		
			w.endRDF();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

}

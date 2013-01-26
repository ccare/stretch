package tools;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

import com.metadatis.stretch.converters.TriplesToAdjacencyList;

public class TriplesToAdj {

	public static void main(String... args) throws Exception {
		final Map<String, String> predicateMap = readPredicates("/tmp/ccare-in/h1.pred");
		String in = "/tmp/ccare-in/h1.nt";
		String out = "/tmp/ccare-in/h1.adj";
		new TriplesToAdjacencyList(predicateMap).convert(new FileInputStream(in),
				new FileOutputStream(out));
	}

	private static Map<String, String> readPredicates(String file) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(file));
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		while(r.ready()) {
			map.put(r.readLine(), Integer.toString(i));
			i++;
		}
		return map;
	}
	
}

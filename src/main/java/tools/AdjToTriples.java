package tools;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.metadatis.stretch.converters.AdjacencyListToTriples;

public class AdjToTriples {

	public static void main(String... args) throws Exception {
		final Map<String, String> predicateMap = readPredicates("/tmp/ccare-in/h1.pred");
		String in = "/tmp/ccare-in/example.out";
		String out = "/tmp/ccare-in/final.nt";
		
		new AdjacencyListToTriples(predicateMap).convert(new FileInputStream(in),
				new FileOutputStream(out));
	}

	private static Map<String, String> readPredicates(String file) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(file));
		Map<String, String> map = new HashMap<String, String>();
		int i = 0;
		while(r.ready()) {
			map.put(Integer.toString(i), r.readLine());
			i++;
		}
		return map;
	}
	
}

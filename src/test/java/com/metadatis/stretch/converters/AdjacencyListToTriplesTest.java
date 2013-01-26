package com.metadatis.stretch.converters;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

public class AdjacencyListToTriplesTest {
	
	AdjacencyListToTriples converter;
	private Map<String, String> predicates;
	
	@Before
	public void setUp() {
		predicates = new HashMap<String, String>();
		predicates.put("1", "http://schema.example.com/predicate1");
		predicates.put("2", "http://schema.example.com/predicate2");
		predicates.put("3", "http://schema.example.com/predicate3");
		converter = new AdjacencyListToTriples(predicates);		
	}

	@Test
	public void shouldConvertSingleRow() throws Exception {
		String in = "http://example.com/s1\t1 http://example.com/objectA\n";
		
		String expected = "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		
		String converted = runConversion(in);

		assertThat(converted, is(equalTo(expected)));
	}
	
	@Test
	public void shouldConvertSingleRowWithMultipleEdges() throws Exception {
		String in = "http://example.com/s1" + 
				"\t1 http://example.com/objectA" +
				"\t1 http://example.com/objectB" +
				"\t2 http://example.com/objectC\n";
		
		String expected;
		expected  = "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		expected += "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectB> .\n";
		expected += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		
		String converted = runConversion(in);

		assertThat(converted, is(equalTo(expected)));
	}
	
	@Test
	public void shouldConvertMultipleRowsWithMultipleEdges() throws Exception {
		String line1 = "http://example.com/s1" + 
				"\t1 http://example.com/objectA" +
				"\t1 http://example.com/objectB" +
				"\t2 http://example.com/objectC\n";
		String line2 = "http://example.com/s2" + 
				"\t2 http://example.com/objectA" +
				"\t2 http://example.com/objectB" +
				"\t2 http://example.com/objectC\n";
		String line3 = "http://example.com/s3" + 
				"\t2 http://example.com/objectA" +
				"\t3 http://example.com/objectB" +
				"\t3 http://example.com/objectC\n";
		
		String expected;
		expected  = "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		expected += "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectB> .\n";
		expected += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		expected += "<http://example.com/s2> <http://schema.example.com/predicate2> <http://example.com/objectA> .\n";
		expected += "<http://example.com/s2> <http://schema.example.com/predicate2> <http://example.com/objectB> .\n";
		expected += "<http://example.com/s2> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		expected += "<http://example.com/s3> <http://schema.example.com/predicate2> <http://example.com/objectA> .\n";
		expected += "<http://example.com/s3> <http://schema.example.com/predicate3> <http://example.com/objectB> .\n";
		expected += "<http://example.com/s3> <http://schema.example.com/predicate3> <http://example.com/objectC> .\n";
		
		String converted = runConversion(line1 + line2 + line3);

		assertThat(converted, is(equalTo(expected)));
	}

	private String runConversion(String input) throws IOException {
		InputStream in = new ByteArrayInputStream(input.getBytes());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		converter.convert(in, out);
		return out.toString();
	}
}

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

public class TriplesToAdjacencyListTest {
	
	TriplesToAdjacencyList converter;
	private Map<String, String> predicates;
	
	@Before
	public void setUp() {
		predicates = new HashMap<String, String>();
		predicates.put("http://schema.example.com/predicate1", "1");
		predicates.put("http://schema.example.com/predicate2", "2");
		predicates.put("http://schema.example.com/predicate3", "3");
		converter = new TriplesToAdjacencyList(predicates);		
	}

	@Test
	public void shouldConvertSingleTriple() throws Exception {
		String triple = "<http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		
		String converted = runConversion(triple);

		String expected = "http://example.com/s1\t1 http://example.com/objectA\n";
		assertThat(converted, is(equalTo(expected)));
	}

	@Test
	public void shouldConvertTriplesWithSameSubject() throws Exception {
		String triple;
		triple = " <http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectB> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		
		String converted = runConversion(triple);

		String expected;
		expected =  "http://example.com/s1";
		expected += "\t1 http://example.com/objectA";
		expected += "\t2 http://example.com/objectB";
		expected += "\t2 http://example.com/objectC\n";
		
		assertThat(converted, is(equalTo(expected)));
	}

	@Test
	public void shouldConvertTriplesWithDifferentSubject() throws Exception {
		String triple;
		triple = " <http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectB> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		triple += "<http://example.com/s2> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		triple += "<http://example.com/s2> <http://schema.example.com/predicate2> <http://example.com/objectD> .\n";
		triple += "<http://example.com/s2> <http://schema.example.com/predicate3> <http://example.com/objectE> .\n";
		
		String converted = runConversion(triple);

		String expected;
		expected =  "http://example.com/s1";
		expected += "\t1 http://example.com/objectA";
		expected += "\t2 http://example.com/objectB";
		expected += "\t2 http://example.com/objectC\n";

		expected +=  "http://example.com/s2";
		expected += "\t2 http://example.com/objectC";
		expected += "\t2 http://example.com/objectD";
		expected += "\t3 http://example.com/objectE\n";
		
		assertThat(converted, is(equalTo(expected)));
	}

	private String runConversion(String input) throws IOException {
		InputStream in = new ByteArrayInputStream(input.getBytes());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		converter.convert(in, out);
		return out.toString();
	}

}

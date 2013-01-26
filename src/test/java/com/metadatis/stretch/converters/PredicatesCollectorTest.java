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

public class PredicatesCollectorTest {
	
	PredicatesCollector converter;
	
	@Before
	public void setUp() {
		converter = new PredicatesCollector();		
	}

	@Test
	public void shouldCollectPredicates() throws Exception {
		String triple;
		triple = " <http://example.com/s1> <http://schema.example.com/predicate1> <http://example.com/objectA> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectB> .\n";
		triple += "<http://example.com/s1> <http://schema.example.com/predicate2> <http://example.com/objectC> .\n";
		triple += "<http://example.com/s2> <http://schema.example.com/predicate3> <http://example.com/objectC> .\n";
		
		String converted = runConversion(triple);

		String expected;
		expected =  "http://schema.example.com/predicate1\n";
		expected += "http://schema.example.com/predicate2\n";
		expected += "http://schema.example.com/predicate3\n";
		
		assertThat(converted, is(equalTo(expected)));
	}
	private String runConversion(String input) throws IOException {
		InputStream in = new ByteArrayInputStream(input.getBytes());
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		converter.convert(in, out);
		return out.toString();
	}

}

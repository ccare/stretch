package com.metadatis.stretch.rdf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

public class UriWritable extends Text {
	
	public UriWritable() {
		super();
	}

	public UriWritable(URI uri) {
		setValue(uri);
	}

	public URI getValue() {
		return new URIImpl(new String(getBytes()));
	}

	public void setValue(URI value) {
		set(value.stringValue());
	}
}

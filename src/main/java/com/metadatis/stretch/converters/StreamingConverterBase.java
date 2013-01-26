package com.metadatis.stretch.converters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class StreamingConverterBase {

	public abstract void convert(final InputStream in, final OutputStream out) throws IOException;
	
}

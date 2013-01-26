package com.metadatis.stretch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.metadatis.stretch.rdf.UriWritable;

public class KodeGraphVertexInputFormat extends TextVertexInputFormat<UriWritable, Text, UriWritable, Text> {
	
	
	@Override
	public org.apache.giraph.io.TextVertexInputFormat.TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new KodeGraphVertexReader();
	}
	
	public class KodeGraphVertexReader extends TextVertexInputFormat.TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		int i = 0;
		@Override
		public Vertex getCurrentVertex() throws IOException,
				InterruptedException {
			final Text currentValue = (Text) getRecordReader().getCurrentValue();
			ByteArrayInputStream in = new ByteArrayInputStream(currentValue.getBytes(), 0 , currentValue.getLength());
			
			Scanner sc = new Scanner(in);
			sc.useDelimiter("\t");
			String id = sc.next();
			KodeGraphVertex v = new KodeGraphVertex();
			v.initialize(new Text(id), new Text());
			
			while (sc.hasNext()) {
				String next = sc.next();
				String[] split = next.split(" ");
				if (split.length < 2) {
					continue;
				}
				v.addEdge(new Text(split[1]), new Text(split[0]));
			}
			return v;		
		}
	}
}
package com.metadatis.stretch.converters.demo;

import java.io.IOException;
import java.util.Collections;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SimpleInputFormat extends TextVertexInputFormat<Text, Text, Text, Text> {

	  public class MyVertexReader
      extends TextVertexInputFormat.TextVertexReader {

		@Override
		public boolean nextVertex() throws IOException, InterruptedException {
			return getRecordReader().nextKeyValue();
		}

		@Override
		public Vertex getCurrentVertex() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			String val = getRecordReader()
	          .getCurrentValue().toString();
			MyVertex myVertex = null;
			String[] edges = val.split(";");
			for (String edge : edges) {
				String[] split = edge.trim().split(" ");
				String nodeLabel = split[0];
				String edgeValue = split[1];
				String nextEdge = split[2];
				if (myVertex == null) {
					myVertex = new MyVertex();
					myVertex.initialize(new Text(nodeLabel), new Text(""));
				}
				if (! nextEdge.equals("_")) {
					myVertex.addEdge(new Text(nextEdge), new Text(edgeValue));
					myVertex.addEdge(new Text(edgeValue), new Text(nextEdge));
				}
			}
			return myVertex;
		}
		  
	  }
	  
	@Override
	public org.apache.giraph.io.TextVertexInputFormat.TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		// TODO Auto-generated method stub
		return new MyVertexReader();
	}


	  
  }
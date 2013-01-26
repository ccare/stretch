package com.metadatis.stretch.converters.demo;

import java.io.IOException;
import java.util.Collections;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyInputFormat extends TextVertexInputFormat<Text, Text, NullWritable, Text> {

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
			// e.g "A(a) B"
			char nodeLabel = val.charAt(0);
			char nodeValue = val.charAt(2);
			char nextEdge = val.charAt(5);
			MyVertex myVertex = new MyVertex();
			myVertex.initialize(new Text("" + nodeLabel), new Text("" + nodeValue));
			if (nextEdge != '_') {
				myVertex.setEdges(Collections.singletonMap(new Text("" + nextEdge), NullWritable.get()));
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
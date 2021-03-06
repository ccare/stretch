package com.metadatis.stretch.chainreduce;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexInputFormat;
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
			ChainReduceVertex myVertex = null;
			String[] edges = val.split(";");
			for (String edge : edges) {
				System.out.println(edge);
				String[] split = edge.trim().split(" ");
				String nodeLabel = split[0];
				String edgeValue = split[1];
				String nextEdge = split[2];
				if (myVertex == null) {
					myVertex = new ChainReduceVertex();
					myVertex.initialize(new Text(nodeLabel), new Text(""));
				}
				if (! nextEdge.equals("_")) {
					myVertex.addEdge(new Text(nextEdge), new Text(edgeValue));
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
package com.metadatis.stretch.converters.demo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MyOutputFormat extends TextVertexOutputFormat<Text, Text, NullWritable> {
	  
	  public class MyVertexWriter
      extends TextVertexOutputFormat.TextVertexWriter {

		@Override
		public void writeVertex(Vertex vertex) throws IOException,
				InterruptedException {
			WritableComparable id = vertex.getId();
			Writable value = vertex.getValue();
			Iterator<Edge> iterator = vertex.getEdges().iterator();
			String nextEdge;
			if (iterator.hasNext()) {
				nextEdge = iterator.next().getTargetVertexId().toString();
			} else {
				nextEdge = "_";
			}
			String output = String.format("%s(%s) %s", id.toString(), 
					value.toString(), nextEdge);
			getRecordWriter().write(output, null);
		}
		  
	  }

	@Override
	public org.apache.giraph.io.TextVertexOutputFormat.TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return new MyVertexWriter();
	}
	  
  }
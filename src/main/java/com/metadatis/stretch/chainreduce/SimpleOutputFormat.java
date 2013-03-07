package com.metadatis.stretch.chainreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SimpleOutputFormat extends TextVertexOutputFormat<Text, Text, NullWritable> {
	  
	  public class MyVertexWriter
      extends TextVertexOutputFormat.TextVertexWriter {

		@Override
		public void writeVertex(Vertex vertex) throws IOException,
				InterruptedException {
			String edgesToExport = getContext().getConfiguration().get("exportEdges", "*");
			if (edgesToExport.equals("*")) {
				WritableComparable id = vertex.getId();
				Writable value = vertex.getValue();
				Iterable<Edge> edges = vertex.getEdges();
				for (Edge<Text, Text> e : edges) {
					Text edgeVal = e.getValue();
					String output = String.format("%s %s %s", id.toString(), 
							edgeVal.toString(), e.getTargetVertexId().toString());
					getRecordWriter().write(output, null);
				}
			} else {
				Set<String> labels = new HashSet<String>();
				labels.addAll(Arrays.asList(edgesToExport.split(",")));
				WritableComparable id = vertex.getId();
				Writable value = vertex.getValue();
				Iterable<Edge> edges = vertex.getEdges();
				for (Edge<Text, Text> e : edges) {
					Text edgeVal = e.getValue();
					if (labels.contains(edgeVal.toString())) {
						String output = String.format("%s %s %s", id.toString(), 
								edgeVal.toString(), e.getTargetVertexId().toString());
						getRecordWriter().write(output, null);
				//		return;
					}
				}
			}
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
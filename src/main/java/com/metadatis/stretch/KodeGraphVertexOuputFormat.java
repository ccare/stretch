package com.metadatis.stretch;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.metadatis.stretch.rdf.UriWritable;

public class KodeGraphVertexOuputFormat extends TextVertexOutputFormat<UriWritable, Text, UriWritable> {

	@Override
	public org.apache.giraph.io.TextVertexOutputFormat.TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		return new KodeGraphVertexWriter();
	}
	
	public class KodeGraphVertexWriter extends org.apache.giraph.io.TextVertexOutputFormat.TextVertexWriter {

		@Override
		public void writeVertex(Vertex vertex) throws IOException,
				InterruptedException {
			Iterable<Edge<Text, Text>> edges = vertex.getEdges();
			StringBuilder sb = new StringBuilder();
			for (Edge<Text, Text> edge : edges) {
				sb.append("\t");
				sb.append(edge.getValue());
				sb.append(" ");
				sb.append(edge.getTargetVertexId());
			}
			getRecordWriter().write(vertex.getId(), 
					new Text(sb.toString()));
		}
		
	}
}

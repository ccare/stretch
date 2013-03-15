package com.metadatis.stretch.chainreduce;

import static com.metadatis.stretch.chainreduce.ChainReduceVertex.*;

import java.util.Map;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

public class ChainReduceUtils {

	public static final Text DUMMY_VERTEX = new Text("X");	
	
	public static boolean noEdge(ChainReduceVertex vertex, Text tag) {
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().equals(tag) && e.getTargetVertexId().equals(DUMMY_VERTEX)) {
				return true;
			}
		}
		return false;
	}

	public static Text findEdgeByValue(ChainReduceVertex vertex, Text tag) {
		Text next = null;
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().equals(tag) && !e.getTargetVertexId().equals(DUMMY_VERTEX)) {
				next = e.getTargetVertexId();
			}
		}
		return next;
	}
}

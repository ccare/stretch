package com.metadatis.stretch.chainreduce;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

public abstract class AbstractChainReduceAction implements VertexAction {

	public AbstractChainReduceAction() {
		super();
	}

	@Override
	public boolean applicable(ChainReduceVertex vertex) {
		boolean reduceCandidate = false;
		String id = vertex.getId().toString();
		if (id.contains("X") && id.length() > 1) {
			reduceCandidate = true;
		}
		return reduceCandidate;
	}
	
	public boolean noEdge(ChainReduceVertex vertex, Text tag) {
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().equals(tag) && e.getTargetVertexId().equals(new Text("X"))) {
				return true;
			}
		}
		return false;
	}

	public Text findEdgeByValue(ChainReduceVertex vertex, Text tag) {
		Text next = null;
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().equals(tag) && !e.getTargetVertexId().equals(new Text("X"))) {
				next = e.getTargetVertexId();
			}
		}
		return next;
	}
}
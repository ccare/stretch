package com.metadatis.stretch.chainreduce.actions;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public abstract class GuardedEdgeAction implements VertexAction<ChainReduceVertex> {

	private final Text edgeLabel;
	private Integer currentHash;
	
	public GuardedEdgeAction(final Text edgeLabel) {
		this.edgeLabel = edgeLabel;
	}

	@Override
	public boolean triggerable(ChainReduceVertex vertex) {
		final int calculated = hashEdgesWithValue(vertex, edgeLabel);
		if (currentHash == null) {
			currentHash = calculated;
			return true;
		} else {
			int current = currentHash.intValue();
			if (current == calculated) {
				return false;
			} else {
				currentHash = calculated;
				return true;
			}
		}
	}	
	
	private int hashEdgesWithValue(ChainReduceVertex vertex, Text tag) {
		int hashCode = 0;
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().equals(tag)) {
				hashCode += e.getTargetVertexId().toString().hashCode();
			}
		}
		return hashCode;
	}
}
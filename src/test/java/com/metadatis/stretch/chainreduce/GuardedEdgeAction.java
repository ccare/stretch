package com.metadatis.stretch.chainreduce;

import org.apache.hadoop.io.Text;

public abstract class GuardedEdgeAction implements VertexAction {

	/**
	 * 
	 */
	private final ChainReduceVertex vertex;
	private final Text edgeLabel;
	private Integer currentHash;
	
	public GuardedEdgeAction(ChainReduceVertex chainReduceVertex, final Text edgeLabel) {
		vertex = chainReduceVertex;
		this.edgeLabel = edgeLabel;
	}

	@Override
	public boolean triggerable() {
		final int calculated = vertex.hashEdgesWithValue(edgeLabel);
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
	
}
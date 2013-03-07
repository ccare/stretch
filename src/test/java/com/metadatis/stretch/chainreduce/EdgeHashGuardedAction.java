package com.metadatis.stretch.chainreduce;

import org.apache.hadoop.io.Text;

abstract class EdgeHashGuardedAction {

	/**
	 * 
	 */
	private final ChainReduceVertex chainReduceVertex;
	private final Text edgeLabel;
	private Integer currentHash;

	public EdgeHashGuardedAction(ChainReduceVertex chainReduceVertex, Text edgeLabel) {
		this.chainReduceVertex = chainReduceVertex;
		this.edgeLabel = edgeLabel;
	}		
	
	protected abstract void action();
	
	public void trigger() {
		if (hashChanged()) {
			action();
		}
	}

	public boolean hashChanged() {
		final int calculated = this.chainReduceVertex.hashEdgesWithValue(edgeLabel);
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
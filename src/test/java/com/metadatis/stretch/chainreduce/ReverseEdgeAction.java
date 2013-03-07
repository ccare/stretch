package com.metadatis.stretch.chainreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;


class ReverseEdgeAction extends GuardedEdgeAction {

	/**
	 * 
	 */
	private final ChainReduceVertex vertex;
	private final Text currentEdgeLabel;
	private final Text targetEdgeLabel;

	public ReverseEdgeAction(ChainReduceVertex chainReduceVertex, Text currentEdgeLabel, Text targetEdgeLabel) {
		super(chainReduceVertex, currentEdgeLabel);
		vertex = chainReduceVertex;
		this.currentEdgeLabel = currentEdgeLabel;
		this.targetEdgeLabel = targetEdgeLabel;
	}

	@Override
	public void trigger() throws IOException {
		vertex.reverseExistingEdge(currentEdgeLabel, targetEdgeLabel);
	}

	@Override
	public boolean finished() {
		return vertex.getSuperstep() > 2;
	}

	@Override
	public boolean applicable() {
		// TODO Auto-generated method stub
		return true;
	}

}
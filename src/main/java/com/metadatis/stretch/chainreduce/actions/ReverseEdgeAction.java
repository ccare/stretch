package com.metadatis.stretch.chainreduce.actions;

import static com.metadatis.stretch.chainreduce.ChainReduceUtils.findEdgeByValue;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class ReverseEdgeAction extends GuardedEdgeAction {

	private final Text currentEdgeLabel;
	private final Text targetEdgeLabel;

	public ReverseEdgeAction(Text currentEdgeLabel, Text targetEdgeLabel) {
		super(currentEdgeLabel);
		this.currentEdgeLabel = currentEdgeLabel;
		this.targetEdgeLabel = targetEdgeLabel;
	}

	@Override
	public void trigger(ChainReduceVertex vertex) throws IOException {
		Text target = findEdgeByValue(vertex, currentEdgeLabel);
		if (target != null) {
			vertex.addEdgeRequest(target, new Edge<Text, Text>(vertex.getId(), targetEdgeLabel));
		}
	}

	@Override
	public boolean finished(ChainReduceVertex vertex) {
		return vertex.getSuperstep() > 2;
	}

	@Override
	public boolean applicable(ChainReduceVertex vertex) {
		return true;
	}
}
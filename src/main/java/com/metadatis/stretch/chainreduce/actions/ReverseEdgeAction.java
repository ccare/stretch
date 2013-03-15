package com.metadatis.stretch.chainreduce.actions;

import static com.metadatis.stretch.chainreduce.ChainReduceUtils.findEdgeByValue;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class ReverseEdgeAction implements VertexAction<ChainReduceVertex> {

	private final String currentEdgeLabelKey;
	private final String targetEdgeLabelKey;

	public ReverseEdgeAction(final String currentEdgeLabelKey, final String targetEdgeLabelKey) {
		this.currentEdgeLabelKey = currentEdgeLabelKey;
		this.targetEdgeLabelKey = targetEdgeLabelKey;
	}

	@Override
	public void trigger(ChainReduceVertex vertex) throws IOException {
		final Text currentEdgeLabel = textFromConfig(vertex, currentEdgeLabelKey);
		final Text targetEdgeLabel = textFromConfig(vertex, targetEdgeLabelKey);
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

	protected Text textFromConfig(ChainReduceVertex vertex, String key) {
		String val = vertex.getConf().get(key);
		return new Text(val);
	}

	@Override
	public boolean triggerable(ChainReduceVertex vertex) {
		return true;
	}
	
	
}
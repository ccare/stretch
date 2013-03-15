package com.metadatis.stretch.chainreduce.actions;

import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public abstract class AbstractChainReduceAction implements VertexAction<ChainReduceVertex> {

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


	protected Text textFromConfig(ChainReduceVertex vertex, String key) {
		String val = vertex.getConf().get(key);
		return new Text(val);
	}

}
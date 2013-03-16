package com.metadatis.stretch.chainreduce.methods;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class IsApplicable implements VertexMethod<Boolean> {
	@Override
	public Boolean calculate(
			ChainReduceVertex vertex, Object... args) {
		final String id = vertex.getId().toString();
		boolean reduceCandidate = false;
		if (id.contains("X") && id.length() > 1) {
			reduceCandidate = true;
		}
		return reduceCandidate;
	}
}
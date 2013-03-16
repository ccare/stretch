package com.metadatis.stretch.chainreduce.methods;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class DeriveParent implements VertexMethod<String> {
	
	@Override
	public String calculate(
			ChainReduceVertex vertex) {
		String[] split = vertex.getId().toString().split("/");
		String derivedParent = split[0];
		return derivedParent;
	}
	
}
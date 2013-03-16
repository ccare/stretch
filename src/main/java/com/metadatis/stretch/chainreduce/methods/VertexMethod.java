package com.metadatis.stretch.chainreduce.methods;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public interface VertexMethod<T> {	
	T calculate(ChainReduceVertex vertex);
}
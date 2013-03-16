package com.metadatis.stretch.chainreduce.actions;

import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;
import com.metadatis.stretch.chainreduce.methods.VertexMethod;

public abstract class AbstractChainReduceAction implements VertexAction<ChainReduceVertex> {

	@Override
	public boolean applicable(ChainReduceVertex vertex) {
		VertexMethod<Boolean> method = methodFromConfig(vertex, ChainReduceVertex.APPLICABLE_CLASS, Boolean.class);
		return method.calculate(vertex);
	}

	protected Text textFromConfig(ChainReduceVertex vertex, String key) {
		final String val = vertex.getConf().get(key);
		return new Text(val);
	}

	public <T> VertexMethod<T> methodFromConfig(ChainReduceVertex vertex, 
			String classKey, Class<T> clazz) {
		final String className = vertex.getConf().get(classKey);
		return vertex.getMethod(className, clazz);
	}

}
package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import com.metadatis.stretch.chainreduce.AbstractActionBasedComputationVertex;

public interface VertexAction<V extends AbstractActionBasedComputationVertex> {

	public boolean triggerable(V vertex);

	public boolean finished(V vertex);

	public void trigger(V vertex) throws IOException;

	public boolean applicable(V vertex);

}
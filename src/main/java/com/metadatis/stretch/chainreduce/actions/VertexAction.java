package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import com.metadatis.stretch.KodeGraphVertex;
import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public interface VertexAction {

	public boolean triggerable(ChainReduceVertex vertex);

	public boolean finished(ChainReduceVertex vertex);

	public void trigger(ChainReduceVertex vertex) throws IOException;

	public boolean applicable(ChainReduceVertex vertex);

}
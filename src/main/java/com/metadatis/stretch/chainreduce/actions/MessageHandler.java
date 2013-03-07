package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import com.metadatis.stretch.chainreduce.AbstractActionBasedComputationVertex;

public interface MessageHandler<V extends AbstractActionBasedComputationVertex> {

	public String getMessageType();

	public void handle(V vertex, String[] params) throws IOException;

}
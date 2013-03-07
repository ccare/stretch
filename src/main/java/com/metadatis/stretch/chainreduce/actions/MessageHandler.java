package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public interface MessageHandler {

	public String getMessageType();

	public void handle(ChainReduceVertex vertex, String[] params) throws IOException;

}
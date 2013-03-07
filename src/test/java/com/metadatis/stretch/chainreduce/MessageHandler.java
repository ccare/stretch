package com.metadatis.stretch.chainreduce;

import java.io.IOException;

interface MessageHandler {

	public String getMessageType();

	public void handle(ChainReduceVertex vertex, String[] params) throws IOException;

}
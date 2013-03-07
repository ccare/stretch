package com.metadatis.stretch.chainreduce;

import java.io.IOException;

interface MessageHandler {

	public String getMessageType();

	public void handle(String[] params) throws IOException;

}
package com.metadatis.stretch.chainreduce;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.metadatis.stretch.chainreduce.actions.MessageHandler;
import com.metadatis.stretch.chainreduce.actions.VertexAction;

public class HandlerRegistry {

	private Map<String, MessageHandler> handlers = new HashMap<String, MessageHandler>();
	private Collection<VertexAction> actions = new HashSet<VertexAction>();

	public void register(Object o) {
		if (o instanceof MessageHandler) {
			MessageHandler messageHandler = (MessageHandler) o;
			handlers.put(messageHandler.getMessageType(), messageHandler);
		}
		if (o instanceof VertexAction) {
			VertexAction action = (VertexAction) o;
			actions.add(action);
		}
	}
	
	public MessageHandler getHandler(String messageType) {
		return handlers.get(messageType);
	}

	public Iterable<VertexAction> getActions() {
		return actions;
	}
}
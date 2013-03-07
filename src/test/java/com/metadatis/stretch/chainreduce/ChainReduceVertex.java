package com.metadatis.stretch.chainreduce;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.Text;

class HandlerRegistry {

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

public class ChainReduceVertex extends MultigraphVertex<Text, Text, Text, Text> {

	private static HandlerRegistry registry = new HandlerRegistry();
	
	static {
		CalculateForwardCandidateAction candidateAction = new CalculateForwardCandidateAction();
		
		registry.register(new ReverseEdgeAction(new Text("p1"), new Text("r1")));
		registry.register(new ReverseEdgeAction(new Text("p2"), new Text("r2")));
		registry.register(candidateAction);
		registry.register(new DiffSameAction(new Text("fSame"), new Text("fDiff"), new Text("cNext"), candidateAction));
		registry.register(new DiffSameAction(new Text("bSame"), new Text("bDiff"), new Text("cPrev"), candidateAction));
	}

	@Override
	public void compute(Iterable<Text> messages) throws IOException {				
		handleMessages(messages);
		invokeActions();
		if (isFinished()) {
			voteToHalt();
		}
	}

	private boolean isFinished() {
		boolean finished = true;
		for (VertexAction a : registry.getActions()) {
			if (a.applicable(this) && ! a.finished(this)) {
				finished = false;
			}
		}
		return finished;
	}

	private void invokeActions() throws IOException {
		for (VertexAction a : registry.getActions()) {
			boolean applicable = a.applicable(this);
			if (applicable) {
				boolean triggerable = a.triggerable(this);
				boolean finished = a.finished(this);
				if (triggerable && ! finished) {
					a.trigger(this);
				}
			}
		}
	}

	private void handleMessages(Iterable<Text> messages) throws IOException {
		for (Text m : messages) {
			final String msg = m.toString();
			final String[] split = msg.split(" ");
			final String msgType = split[0];
			MessageHandler messageHandler = registry.getHandler(msgType);
			if (messageHandler != null) {
				messageHandler.handle(this, split);
			}
		}
	}

}
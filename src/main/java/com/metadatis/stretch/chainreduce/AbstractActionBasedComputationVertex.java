package com.metadatis.stretch.chainreduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.actions.MessageHandler;
import com.metadatis.stretch.chainreduce.actions.VertexAction;

public abstract class AbstractActionBasedComputationVertex extends
		MultigraphVertex<Text, Text, Text, Text> {

	protected static HandlerRegistry registry = new HandlerRegistry();

	public AbstractActionBasedComputationVertex() {
		super();
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
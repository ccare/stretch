package com.metadatis.stretch.chainreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.EdgeListVertex;
import org.apache.giraph.graph.HashMapVertex;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChainReduceVertex extends MultigraphVertex<Text, Text, Text, Text> {

	public static StringBuffer sb = new  StringBuffer();
	
	private void log(String msg, Object... args) {
		sb.append(String.format("%s %s", getSuperstep(), getId()));
		sb.append(' ');
		sb.append(String.format(msg, args));
		sb.append('\n');
	}
	
	private AtomicLong version = new AtomicLong();
	private long last = -1;
	
	@Override
	public void sendMessage(Text id, Text message) {
		String exchange = String.format("(STEP %d) SEND %s->%s <%s>\n", getSuperstep(), getId(), id, message );
		sb.append(exchange);
		super.sendMessage(id, message);
	}

	
	@Override
	public boolean addEdge(Text targetVertexId, Text value) {
		Long superstep;
		try {
			superstep = getSuperstep();
		} catch (NullPointerException e) {
			superstep = null;
		}
		String exchange = String.format("(STEP %d) ADD %s--%s-->%s\n", superstep, getId(), value, targetVertexId );
		sb.append(exchange);
		return super.addEdge(targetVertexId, value);
	}

	public void sendEdgeRequest(Text sourceVertexId, Edge<Text, Text> edge)
			throws IOException {
		String msg = String.format("NEW %s %s", edge.getTargetVertexId(), edge.getValue() );
	//	sendMessage(sourceVertexId, new Text(msg));
		addEdgeRequest(sourceVertexId, edge);
	}


	String myValue = null;
	boolean complete = false;

	private final String derivedEdgeValue = "p4";
	// private final String reverseDerivedEdgeValue = "r4";
	// private final String candidateEdgeValue = "c5";
	//private final String reverseCandidateEdgeValue = "rc5";
	// private final String previousCandidateEdgeValue = "r6";

	private Map<String, MessageHandler> handlers = new HashMap<String, MessageHandler>();
	private Collection<VertexAction> actions = new HashSet<VertexAction>();
	private Integer hash;
	{
		CalculateForwardCandidateAction candidateAction = new CalculateForwardCandidateAction(this);
		
		register(new ReverseEdgeAction(this, new Text("p1"), new Text("r1")));
		register(new ReverseEdgeAction(this, new Text("p2"), new Text("r2")));
		register(candidateAction);
		register(new DiffSameAction(this, new Text("fSame"), new Text("fDiff"), new Text("cNext"), candidateAction));
		register(new DiffSameAction(this, new Text("bSame"), new Text("bDiff"), new Text("cPrev"), candidateAction));
	}

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

	@Override
	public void compute(Iterable<Text> messages) throws IOException {
				
		if (getSuperstep() > 20) {
			voteToHalt();
			return;
		}
				
		for (Text m : messages) {
			final String msg = m.toString();
			final String[] split = msg.split(" ");
			final String msgType = split[0];
			MessageHandler messageHandler = handlers.get(msgType);
			if (messageHandler != null) {
				messageHandler.handle(split);
			}
		}
		
		for (VertexAction a : actions) {
			boolean applicable = a.applicable();
			if (applicable) {
				boolean triggerable = a.triggerable();
				boolean finished = a.finished();
				log("%s: Triggerable %s and finished %s", 
						a.getClass().getName(), triggerable, finished);
				if (triggerable && ! finished) {
					log("Triggering %s. Triggerable %s and finished %s", 
							a.getClass().getName(), triggerable, finished);
					a.trigger();
				}
			}
		}
		
		boolean finished = true;
		
		for (VertexAction a : actions) {
			if (a.applicable() && ! a.finished()) {
				log("%s NOT FINISHED", a.getClass().getName());
				finished = false;
			}
		}
		
		if (finished) {
			voteToHalt();
		}

	}
	
	void reverseExistingEdge(Text currentLabel, Text targetLabel)
			throws IOException {
		Text target = findEdge(currentLabel);
		if (target != null) {
			newEdge(target, getId(), targetLabel);
		//	nudge(target);
		}
	}

	private void newEdge(Text from, Text to, Text label) throws IOException {
		sendEdgeRequest(from, new Edge<Text, Text>(to, label));
	}


	Text findEdge(Text tag) {
		Text next = null;
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().equals(tag) && !e.getTargetVertexId().equals(new Text("X"))) {
				next = e.getTargetVertexId();
			}
		}
		return next;
	}

	boolean noEdge(Text tag) {
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().equals(tag) && e.getTargetVertexId().equals(new Text("X"))) {
				sb.append("Found _ node for tag " + tag + "\n");
				return true;
			}
		}
		return false;
	}
	
	private int hashEdges() {
		int hashCode = 0;
		for (Edge<Text, Text> e : getEdges()) {
			hashCode += e.getTargetVertexId().toString().hashCode();
		}
		return hashCode;
	}

	int hashEdgesWithValue(Text tag) {
		int hashCode = 0;
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().equals(tag)) {
				hashCode += e.getTargetVertexId().toString().hashCode();
			}
		}
		return hashCode;
	}

	private void cascadeNudge(String cascadeTo) {
		cascadeNudge(new Text(cascadeTo));
	}

	private void cascadeNudge(Text cascadeTo) {
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().equals(cascadeTo)) {
				Text target = e.getTargetVertexId();
				nudge(target);
			}
		}
	}

	void nudge(Text target) {
		if (target != null) {
			sendMessage(target, new Text("_"));
		}
	}

	//
	boolean isReduceCandidate() {
		boolean reduceCandidate = false;
		String id = getId().toString();
		if (id.contains("X") && id.length() > 1) {
			reduceCandidate = true;
		}
		return reduceCandidate;
	}

	String calculateMyValue() {
		String myvalue = null;
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().toString().equals("p3")) {
				myvalue = e.getTargetVertexId().toString();
			}
		}
		return myvalue;
	}
}
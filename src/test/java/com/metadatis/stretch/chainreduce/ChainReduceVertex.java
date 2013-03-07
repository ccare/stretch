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
		CalculateForwardCandidateAction candidateAction = new CalculateForwardCandidateAction();
		
		register(new ReverseEdgeAction(new Text("p1"), new Text("r1")));
		register(new ReverseEdgeAction(new Text("p2"), new Text("r2")));
		register(candidateAction);
		register(new DiffSameAction(new Text("fSame"), new Text("fDiff"), new Text("cNext"), candidateAction));
		register(new DiffSameAction(new Text("bSame"), new Text("bDiff"), new Text("cPrev"), candidateAction));
//		register(new ChainReduceForward());
//		register(new CascadeNotifier(new Text("cPrev")));
//		register(new NudgeListener());
//		register(new CalculateForwardDiffAction(new Text("fSame"), new Text("fDiff"), new Text("cNext")));// < Adding this breaks the cNext
//		register(new CalculateForwardDiffAction(new Text("bSame"), new Text("bDiff"), new Text("cPrev")));// <- Adding this results in spin 
//		register(new EdgeRequestListener());

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
		
	//	if (last != version.get()) {	
		//	last = version.get();
			for (VertexAction a : actions) {
			//	log("is action %s triggerable", a.getClass().getName());
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
	//	}
		
		boolean finished = true;
		
		for (VertexAction a : actions) {
			if (! a.finished()) {
				log("%s NOT FINISHED", a.getClass().getName());
				finished = false;
			}
		}
		
		if (finished) {
			voteToHalt();
		}
		

//		int current = hashEdges();
//		if (hash == null || !hash.equals(current)) {
//			hash = current;
//			for (VertexAction a : actions) {
//				if (a.triggerable()) {
//					a.trigger();
//				}
//			}
//		} else {
//			voteToHalt();
//		}

	}
	
	abstract class EdgeHashGuardedAction {

		private final Text edgeLabel;
		private Integer currentHash;

		public EdgeHashGuardedAction(Text edgeLabel) {
			this.edgeLabel = edgeLabel;
		}		
		
		protected abstract void action();
		
		public void trigger() {
			if (hashChanged()) {
				action();
			}
		}

		public boolean hashChanged() {
			final int calculated = hashEdgesWithValue(edgeLabel);
			if (currentHash == null) {
				currentHash = calculated;
				return true;
			} else {
				int current = currentHash.intValue();
				if (current == calculated) {
					return false;
				} else {
					currentHash = calculated;
					return true;
				}
			}
		}

	}


	abstract class GuardedEdgeAction implements VertexAction {

		private final Text edgeLabel;
		private Integer currentHash;
		
		public GuardedEdgeAction(final Text edgeLabel) {
			this.edgeLabel = edgeLabel;
		}

		@Override
		public boolean triggerable() {
			final int calculated = hashEdgesWithValue(edgeLabel);
			if (currentHash == null) {
				currentHash = calculated;
				return true;
			} else {
				int current = currentHash.intValue();
				if (current == calculated) {
					return false;
				} else {
					currentHash = calculated;
					return true;
				}
			}
		}	
		
	}
	
	class NudgeListener implements MessageHandler {

		@Override
		public String getMessageType() {
			return "_";
		}

		@Override
		public void handle(String[] params) throws IOException {
			hash = null;
		}
		
	}
	
	class EdgeRequestListener implements MessageHandler {

		@Override
		public String getMessageType() {
			return "NEW";
		}

		@Override
		public void handle(String[] params) throws IOException {
			boolean addEdge = addEdge(new Text(params[1]), new Text(params[2]));
			if (addEdge) {
				version.incrementAndGet();
			}
		}
		
	}
	
	
	class ReverseEdgeAction extends GuardedEdgeAction {

		private final Text currentEdgeLabel;
		private final Text targetEdgeLabel;

		public ReverseEdgeAction(Text currentEdgeLabel, Text targetEdgeLabel) {
			super(currentEdgeLabel);
			this.currentEdgeLabel = currentEdgeLabel;
			this.targetEdgeLabel = targetEdgeLabel;
		}

		@Override
		public void trigger() throws IOException {
			reverseExistingEdge(currentEdgeLabel, targetEdgeLabel);
		}

		@Override
		public boolean finished() {
			return getSuperstep() > 2;
		}

	}

	class CalculateForwardCandidateAction implements VertexAction,
			MessageHandler {

		private final Text DUMMY_VERTEX = new Text("X");
		
		Text p1 = new Text("p1");
		Text r1 = new Text("r1");
		Text next = new Text("cNext");
		Text prev = new Text("cPrev");
		
		@Override
		public boolean triggerable() {
			Text target = findEdge(next);
			//boolean noSuchEdge = noEdge(next);
			return isReduceCandidate() && target == null;// && !noSuchEdge;
		}

		@Override
		public void trigger() throws IOException {
			String vertexId = getId().toString();
			String[] split = vertexId.split("/");
			String derivedParent = split[0];
			Text msg = new Text("FIND_NEXT " + vertexId);
			sendMessage(new Text(derivedParent), msg);
		}

		@Override
		public String getMessageType() {
			return "FIND_NEXT";
		}

		@Override
		public void handle(String[] params) throws IOException {
			final Text src = new Text(params[1]);
			Text pTargetId = findEdge(p1);
			if (pTargetId != null) {
				Text candidate = deriveEquivalentNode(src, pTargetId);
				sendEdgeRequest(src, new Edge(candidate, next));
				sendEdgeRequest(candidate, new Edge(src, prev));
				nudge(new Text(candidate));
			} else {
				sendEdgeRequest(src, new Edge(DUMMY_VERTEX, next));				
			}
			Text rTargetId = findEdge(r1);
			if (rTargetId != null) {
				Text candidate = deriveEquivalentNode(src, rTargetId);
				sendEdgeRequest(src, new Edge(candidate, prev));
				sendEdgeRequest(candidate, new Edge(src, next));
				nudge(new Text(candidate));
			} else {
				sendEdgeRequest(src, new Edge(DUMMY_VERTEX, prev));				
			}
			nudge(new Text(src));
		}

		private Text deriveEquivalentNode(Text vertexId, Text result) {
			String[] idSplit = vertexId.toString().split("/");
			String identifierFragment = idSplit[1];
			String candidate = String.format("%s/%s", result.toString(),
					identifierFragment);
			return new Text(candidate);
		}

		@Override
		public boolean finished() {
			if (!isReduceCandidate()) {
				return true;
			}
			Text target = findEdge(next);
			boolean foundTarget = target != null;
			boolean noSuchEdge = noEdge(next);
//			if (getId().toString().equals("G/X")) {
//				return true;
//			}
			sb.append(String.format("%s: %s\n", getId(), foundTarget || noSuchEdge));
			return (foundTarget || noSuchEdge);
		}
	}

	class DiffSameAction implements VertexAction,
	MessageHandler {
		
		private final CalculateForwardCandidateAction candidateAction;
		private final Text sameLabel;
		private final Text diffLabel;
		private final Text candidateLabel;
		
		boolean done = false;
		
		public DiffSameAction(Text sameLabel, Text diffLabel, final Text candidateLabel, CalculateForwardCandidateAction candidateAction) {
			this.sameLabel = sameLabel;
			this.diffLabel = diffLabel;
			this.candidateLabel = candidateLabel;
			this.candidateAction = candidateAction;
		}

		@Override
		public String getMessageType() {
			return "DIFF_SAME_" + candidateLabel;
		}

		@Override
		public void handle(String[] params) throws IOException {
			String src = params[1];
			String value = params[2];
			
			String calculateMyValue = calculateMyValue();
			
			Text sourceVertexId = new Text(src);
			if (calculateMyValue.equals(value)) {
				sendEdgeRequest(sourceVertexId, new Edge(getId(), sameLabel));
			} else {
				sendEdgeRequest(sourceVertexId, new Edge(getId(),  diffLabel));				
			}			
		}

		@Override
		public boolean triggerable() {
			if (! isReduceCandidate()) {
				return false;
			} else if (!candidateAction.finished()) {
				return false;
			} else if (null == findEdge(candidateLabel)) {
				return false;
			} else if (noEdge(candidateLabel)) {
				return false;
			}  else {
				return true;
			}
		}

		@Override
		public boolean finished() {
			if (!isReduceCandidate()) {
				return true;
			} else if (!candidateAction.finished()) {
				return false;
			} else {
				return null != findEdge(sameLabel) || null != findEdge(diffLabel);
			}
		}

		@Override
		public void trigger() throws IOException {
			String vertexId = getId().toString();
			Text candidate = findEdge(candidateLabel);
			Text msg = new Text(getMessageType() + " " + vertexId + " " + calculateMyValue());
			sendMessage(candidate, msg);
			done = true;
		}
		
	}

	class CalculateForwardDiffAction implements VertexAction,
			MessageHandler {

		private final Text sameLabel;
		private final Text diffLabel;
		private final Text candidateLabel;
		

		public CalculateForwardDiffAction(Text sameLabel, Text diffLabel,
				Text candidateLabel) {
			super();
			this.sameLabel = sameLabel;
			this.diffLabel = diffLabel;
			this.candidateLabel = candidateLabel;
		}

		@Override
		public String getMessageType() {
			return "IS_SAME";
		}

		@Override
		public void handle(String[] params) throws IOException {
			String src = params[1];
			String value = params[2];
			
			String calculateMyValue = calculateMyValue();
			
			Text sourceVertexId = new Text(src);
			if (calculateMyValue.equals(value)) {
				sendEdgeRequest(sourceVertexId, new Edge(getId(), sameLabel));
			} else {
				sendEdgeRequest(sourceVertexId, new Edge(getId(),  diffLabel));				
			}			
		}
		
		@Override
		public boolean triggerable() {
			Text candidate = findEdge(candidateLabel);
			Text same = findEdge(sameLabel);
			Text diff = findEdge(diffLabel);
			boolean hasCandidateEdge = candidate != null;
			if (! isReduceCandidate()) {
				return false;
			} else if (! hasCandidateEdge) {
				return false;
			} else {
				return true;
			}
			//return isReduceCandidate() && hasCandidateEdge;
		}

		@Override
		public void trigger() throws IOException {
			String vertexId = getId().toString();
			Text candidate = findEdge(candidateLabel);
			Text msg = new Text("IS_SAME " + vertexId + " " + calculateMyValue());
			sendMessage(candidate, msg);
		}

		@Override
		public boolean finished() {
			if (!isReduceCandidate()) {
				return true;
			} 
			log("Has %s finished?", getClass().getSimpleName());
			log("Looking for candidate %s", candidateLabel);
			Text candidate = findEdge(candidateLabel);
			log("Found %s", candidate);
			boolean finished;
			if (candidate == null) {
				log("no candidate");
				if (noEdge(candidateLabel)) {
					log("no edge");
					finished = true;
				} else {
					log("more");
					finished = false;
				}
			} else {
				log("candidate: %s", candidate);
				Text same = findEdge(sameLabel);
				Text diff = findEdge(diffLabel);
				if (same == null && diff == null) {
					log("no labels");
					finished = false;
				} else {
					log("found");
					finished = true;
				}	
			}
			log("%s finished = %s", getClass().getSimpleName(), finished);
			return finished;
		}

		
	}
	
	
	class CascadeNotifier extends EdgeHashGuardedAction implements MessageHandler {
		
		private final Text edgeLabel;

		public CascadeNotifier(final Text edgeLabel) {
			super(edgeLabel);
			this.edgeLabel = edgeLabel;
		}

		@Override
		public String getMessageType() {
			return "NOTIFY_CASCADE";
		}

		@Override
		public void handle(String[] params) throws IOException {
			final String cascadeTo = params[1];
			if (edgeLabel.toString().equals(cascadeTo)) {
				trigger();
			} else {
				throw new IllegalArgumentException("Bad cascade");
			}
		}

		@Override
		public void action() {
			cascadeNudge(edgeLabel);
		}
		
		
		
	}
	
	class ChainReduceForward extends GuardedEdgeAction implements VertexAction, MessageHandler {

		public ChainReduceForward() {
			super(new Text("cNext"));
		}

		@Override
		public String getMessageType() {
			return "REDUCE";
		}

		@Override
		public void handle(String[] params) throws IOException {
			final String myvalue = calculateMyValue();
			final String tag = params[1];
			final String valueForComparison = params[2];
			final String src = params[3];
			final String reverseTag = params[4];
			chainReduce(getId(), myvalue, new Text(tag), valueForComparison,
					new Text(src), new Text(reverseTag));
		}

		private void chainReduce(Text vertexId, String myvalue, Text tag,
				String valueForComparison, Text srcVertex, Text reverseTag)
				throws IOException {
			final Text result;
			if (!valueForComparison.equals(myvalue)) {
				result = vertexId;
			} else {
				result = findEdge(tag);
			}
			if (result != null) {
				sendEdgeRequest(srcVertex, new Edge(result, tag));
	//			nudge(srcVertex);
				sendMessage(srcVertex,
						new Text("NOTIFY_CASCADE " + reverseTag.toString()));
			}
		}

		@Override
		public boolean triggerable() {
			if (!super.triggerable()) {
				return false;
			} else {
				Text derived = findEdge(new Text(derivedEdgeValue));
				Text candidate = findEdge(new Text("cNext"));
				return isReduceCandidate() && derived == null && candidate != null;
			}
		}

		@Override
		public void trigger() throws IOException {
			Text candidate = findEdge(new Text("cNext"));
			final String myvalue = calculateMyValue();
			Text message = new Text("REDUCE " + derivedEdgeValue + " "
					+ myvalue + " " + getId().toString() + " "
					+ "cPrev");
			sendMessage(candidate, message);
		}

		@Override
		public boolean finished() {
			// TODO Auto-generated method stub
			return false;
		}
	}

	// @Override
	// public void compute(Iterable<Text> messages) throws IOException {
	//
	// final String myvalue = calculateMyValue();
	//
	// for (Text m : messages) {
	// final String msg = m.toString();
	// final String[] split = msg.split(" ");
	// final String msgType = split[0];
	// if (msgType.equals("NOTIFY_CASCADE")) {
	// final String cascadeTo = split[1];
	// cascadeNudge(cascadeTo);
	// } else if (msgType.equals("FIND_NEXT")) {
	// final String tag = split[1];
	// final String src = split[2];
	// final String candidateEdgeValue = split[3];
	// final String reverseCandidateEdgeValue = split[4];
	// constructCandidateChain(new Text(tag), new Text(src),
	// new Text(candidateEdgeValue),
	// new Text(reverseCandidateEdgeValue));
	// } else if (msgType.equals("BUILD_SAME_LINKS")) {
	// final String tag = split[1];
	// final String valueForComparison = split[2];
	// final String src = split[3];
	// final String reverseTag = split[4];
	// buildDiffLinks(getId(), myvalue, new Text(tag), valueForComparison,
	// new Text(src), new Text(reverseTag));
	// } else if (msgType.equals("REDUCE")) {
	// final String tag = split[1];
	// final String valueForComparison = split[2];
	// final String src = split[3];
	// final String reverseTag = split[4];
	// chainReduce(getId(), myvalue, new Text(tag), valueForComparison,
	// new Text(src), new Text(reverseTag));
	// } else if (msgType.equals("REDUCE_BACKWARD")) {
	// final Text tag = new Text(split[1]); // backward link
	// final String valueForComparison = split[2]; // val
	// final Text src = new Text(split[3]);
	// final Text previousCandidateEdgeValue = new Text(split[4]); // forward
	// link
	// Text prev = findEdge(previousCandidateEdgeValue);
	//
	// Text backSame = findEdge(new Text("sameVersionBackward"));
	// Text backDiff = findEdge(new Text("diffVersionBackward"));
	// Text forwardSame = findEdge(new Text("sameVersionForward"));
	// Text forwardDiff = findEdge(new Text("diffVersionForward"));
	// // nudge(forwardSame);
	// // nudge(forwardDiff);
	// // nudge(backDiff);
	// // nudge(backSame);
	// if (forwardDiff != null) {
	// prev = forwardDiff;
	// } else if (forwardSame != null) {
	// prev = forwardSame;
	// }
	// Text result = null;
	// if (valueForComparison.equals(myvalue)) {
	// Text localAnwer = findEdge(tag);
	// if (prev == null && localAnwer != null) {
	// result = getId();
	// }
	//
	// } else {
	// if (prev != null) {
	// result = prev;
	// }
	// }
	// if (result != null) {
	// sendEdgeRequest(src, new Edge(result, tag));
	// }
	// }
	// }
	//
	// // Is this a reduce candidate
	// final boolean reduceCandidate = isReduceCandidate();
	// final String vertexId = getId().toString();
	//
	// Text backSame = findEdge(new Text("sameVersionBackward"));
	// Text backDiff = findEdge(new Text("diffVersionBackward"));
	// Text forwardSame = findEdge(new Text("sameVersionForward"));
	// Text forwardDiff = findEdge(new Text("diffVersionForward"));
	//
	// copyExistingEdge(new Text("sameVersionForward"), new Text("p6"));
	// copyExistingEdge(new Text("diffVersionForward"), new Text("p7"));
	// copyExistingEdge(new Text("diffVersionBackward"), new Text("r7"));
	//
	// reverseExistingEdge(new Text("p1"), new Text("r1"));
	// reverseExistingEdge(new Text("p2"), new Text("r2"));
	// reverseExistingEdge(new Text("p6"), new Text("r6"));
	// // reverseExistingEdge(new Text("r6"), new Text("p6"));
	// // reverseExistingEdge(new Text("r7"), new Text("p7"));
	//
	// String candidate = null;
	// String derived = null;
	// String reverseDerived = null;
	// String reverseCandidate = null;
	// for (Edge<Text, Text> e : getEdges()) {
	// if (e.getValue().toString().equals(candidateEdgeValue)) {
	// candidate = e.getTargetVertexId().toString();
	// }
	// if (e.getValue().toString().equals(derivedEdgeValue)) {
	// derived = e.getTargetVertexId().toString();
	// }
	// if (e.getValue().toString().equals(reverseDerivedEdgeValue)) {
	// reverseDerived = e.getTargetVertexId().toString();
	// }
	// if (e.getValue().toString().equals(reverseCandidateEdgeValue)) {
	// reverseCandidate = e.getTargetVertexId().toString();
	// }
	// }
	//
	// boolean noBack = backDiff == null && backSame == null;
	// boolean noForward = forwardDiff == null && forwardSame == null;
	// if (reduceCandidate && candidate != null && noForward) {
	// Text message = new Text("BUILD_SAME_LINKS "
	// + derivedEdgeValue
	// + " "
	// + myvalue
	// + " "
	// + vertexId
	// + " "
	// + reverseCandidateEdgeValue);
	// sendMessage(new Text(candidate), message);
	// }
	//
	// if (reduceCandidate && reverseCandidate != null && noBack) {
	// nudge(new Text(reverseCandidate));
	// }
	//
	//
	// if (reduceCandidate && derived != null) {
	//
	// } else if (reduceCandidate && candidate == null) {
	// String[] split = vertexId.split("/");
	// String derivedParent = split[0];
	// Text msg = new Text("FIND_NEXT p1 " + vertexId
	// + " " + candidateEdgeValue + " " + reverseCandidateEdgeValue);
	// sendMessage(new Text(derivedParent), msg);
	// } else {
	// compute(myvalue, reduceCandidate, vertexId,
	// candidate, derived,
	// derivedEdgeValue, reverseCandidateEdgeValue);
	//
	// }
	//
	// if (candidate != null && noForward) {
	// nudge(new Text(candidate));
	// } else if (reverseCandidate != null && noBack) {
	// nudge(new Text(reverseCandidate));
	// } else {
	// voteToHalt();
	// }
	// }
	//
	//
	//
	// private void compute(String myvalue, boolean reduceCandidate,
	// String vertexId, String candidate, String derived,
	// String derivedEdgeValue, String reverseCandidateEdgeValue) {
	// if (reduceCandidate && derived == null && candidate != null) {
	// Text message = new Text("REDUCE "
	// + derivedEdgeValue
	// + " "
	// + myvalue
	// + " "
	// + vertexId
	// + " "
	// + reverseCandidateEdgeValue);
	// sendMessage(new Text(candidate), message);
	// }
	// }
	//
	private void reverseExistingEdge(Text currentLabel, Text targetLabel)
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


	private Text findEdge(Text tag) {
		Text next = null;
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().equals(tag) && !e.getTargetVertexId().equals(new Text("X"))) {
				next = e.getTargetVertexId();
			}
		}
		return next;
	}

	private boolean noEdge(Text tag) {
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

	private int hashEdgesWithValue(Text tag) {
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

	private void nudge(Text target) {
		if (target != null) {
			sendMessage(target, new Text("_"));
		}
	}

	//
	private boolean isReduceCandidate() {
		boolean reduceCandidate = false;
		String id = getId().toString();
		if (id.contains("X") && id.length() > 1) {
			reduceCandidate = true;
		}
		return reduceCandidate;
	}

	private String calculateMyValue() {
		String myvalue = null;
		for (Edge<Text, Text> e : getEdges()) {
			if (e.getValue().toString().equals("p3")) {
				myvalue = e.getTargetVertexId().toString();
			}
		}
		return myvalue;
	}
}

interface MessageHandler {

	public String getMessageType();

	public void handle(String[] params) throws IOException;

}

interface VertexAction {

	public boolean triggerable();

	public boolean finished();

	public void trigger() throws IOException;

}
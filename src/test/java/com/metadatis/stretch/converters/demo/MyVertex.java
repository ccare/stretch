package com.metadatis.stretch.converters.demo;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.HashMapVertex;
import org.apache.hadoop.io.Text;

public class MyVertex extends HashMapVertex<Text, Text, Text, Text> {

		String myValue = null;
		boolean complete = false;
		
		private final String derivedEdgeValue = "p4";
		private final String reverseDerivedEdgeValue = "r4";
		private final String candidateEdgeValue = "c5";
		private final String reverseCandidateEdgeValue = "rc5";
		private final String previousCandidateEdgeValue = "r6";

		@Override
		public void compute(Iterable<Text> messages) throws IOException {
			
			final String myvalue = calculateMyValue();
						
			for (Text m : messages) {
				final String msg = m.toString();
				final String[] split = msg.split(" ");
				final String msgType = split[0];
				if (msgType.equals("NOTIFY_CASCADE")) {
					final String cascadeTo = split[1];
					cascadeNudge(cascadeTo);
				} else if (msgType.equals("FIND_NEXT")) {
					final String tag = split[1];
					final String src = split[2];
					final String candidateEdgeValue = split[3];
					final String reverseCandidateEdgeValue = split[4];
					constructCandidateChain(new Text(tag), new Text(src), 
							new Text(candidateEdgeValue),
							new Text(reverseCandidateEdgeValue));
				} else if (msgType.equals("BUILD_SAME_LINKS")) {
					final String tag = split[1];
					final String valueForComparison = split[2];
					final String src = split[3];
					final String reverseTag = split[4];
					buildDiffLinks(getId(), myvalue, new Text(tag), valueForComparison, 
							new Text(src), new Text(reverseTag));
				} else if (msgType.equals("REDUCE")) {
					final String tag = split[1];
					final String valueForComparison = split[2];
					final String src = split[3];
					final String reverseTag = split[4];
					chainReduce(getId(), myvalue, new Text(tag), valueForComparison, 
							new Text(src), new Text(reverseTag));
				} else if (msgType.equals("REDUCE_BACKWARD")) {
					final Text tag = new Text(split[1]); // backward link
					final String valueForComparison = split[2]; // val
					final Text src = new Text(split[3]); 
					final Text previousCandidateEdgeValue = new Text(split[4]); // forward link
					Text prev = findEdge(previousCandidateEdgeValue);
					
					Text backSame = findEdge(new Text("sameVersionBackward"));
					Text backDiff = findEdge(new Text("diffVersionBackward"));
					Text forwardSame = findEdge(new Text("sameVersionForward"));
					Text forwardDiff = findEdge(new Text("diffVersionForward"));
//					nudge(forwardSame);
//					nudge(forwardDiff);
//					nudge(backDiff);
//					nudge(backSame);
					if (forwardDiff != null) {
						prev = forwardDiff;
					} else if (forwardSame != null) {
						prev = forwardSame;
				}
					Text result = null;
					if (valueForComparison.equals(myvalue)) {
						Text localAnwer = findEdge(tag);
						if (prev == null && localAnwer != null) {
							result = getId();
			}
			
					} else {
						if (prev != null) {
							result = prev;
						}
					}
					if (result != null) {
						addEdgeRequest(src, new Edge(result, tag));
					}
				}
			}

			// Is this a reduce candidate
			final boolean reduceCandidate = isReduceCandidate();
			final String vertexId = getId().toString();
			
			Text backSame = findEdge(new Text("sameVersionBackward"));
			Text backDiff = findEdge(new Text("diffVersionBackward"));
			Text forwardSame = findEdge(new Text("sameVersionForward"));
			Text forwardDiff = findEdge(new Text("diffVersionForward"));
						
			copyExistingEdge(new Text("sameVersionForward"), new Text("p6"));
			copyExistingEdge(new Text("diffVersionForward"), new Text("p7"));
			copyExistingEdge(new Text("diffVersionBackward"), new Text("r7"));

			reverseExistingEdge(new Text("p1"), new Text("r1"));
			reverseExistingEdge(new Text("p2"), new Text("r2"));
			reverseExistingEdge(new Text("p6"), new Text("r6"));
		//	reverseExistingEdge(new Text("r6"), new Text("p6"));
		//	reverseExistingEdge(new Text("r7"), new Text("p7"));
						
			String candidate  = null;
			String derived  = null;
			String reverseDerived  = null;
			String reverseCandidate  = null;
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals(candidateEdgeValue)) {
					candidate = e.getTargetVertexId().toString();
				}
				if (e.getValue().toString().equals(derivedEdgeValue)) {
					derived = e.getTargetVertexId().toString();
				}
				if (e.getValue().toString().equals(reverseDerivedEdgeValue)) {
					reverseDerived = e.getTargetVertexId().toString();
				}
				if (e.getValue().toString().equals(reverseCandidateEdgeValue)) {
					reverseCandidate = e.getTargetVertexId().toString();
				}
			}

			boolean noBack = backDiff == null && backSame == null;
			boolean noForward = forwardDiff == null && forwardSame == null;
			if (reduceCandidate && candidate != null && noForward) {
				Text message = new Text("BUILD_SAME_LINKS " 
						+ derivedEdgeValue
						+ " "
						+ myvalue 
						+ " " 
						+ vertexId
						+ " "
						+ reverseCandidateEdgeValue);
				sendMessage(new Text(candidate), message);		
			}
			
			if (reduceCandidate && reverseCandidate != null && noBack) {
				nudge(new Text(reverseCandidate));
			}
			
			
			if (reduceCandidate && derived != null) {
				
			} else if (reduceCandidate && candidate == null) {
				String[] split = vertexId.split("/");
				String derivedParent = split[0];
				Text msg = new Text("FIND_NEXT p1 " + vertexId 
						+ " " + candidateEdgeValue + " " + reverseCandidateEdgeValue);
				sendMessage(new Text(derivedParent), msg);
			} else {
				compute(myvalue, reduceCandidate, vertexId, 
						candidate, derived, 
						derivedEdgeValue, reverseCandidateEdgeValue);

			}
			
			if (candidate != null && noForward) {
				nudge(new Text(candidate));
			} else if (reverseCandidate != null && noBack) {
				nudge(new Text(reverseCandidate));
			} else {
				voteToHalt();
			}
		}

				

		private void compute(String myvalue, boolean reduceCandidate,
				String vertexId, String candidate, String derived,
				String derivedEdgeValue, String reverseCandidateEdgeValue) {
				if (reduceCandidate && derived == null && candidate != null) {
					Text message = new Text("REDUCE " 
							+ derivedEdgeValue
							+ " "
							+ myvalue 
							+ " " 
							+ vertexId
							+ " "
							+ reverseCandidateEdgeValue);
					sendMessage(new Text(candidate), message);
				}
		}

		private void reverseExistingEdge(Text currentLabel, Text targetLabel) throws IOException {
			Text next = findEdge(currentLabel);
			if (next != null) {
				addEdgeRequest(next, new Edge<Text, Text>(getId(), targetLabel));
			}
		}
		
		private void copyExistingEdge(Text currentLabel, Text targetLabel) throws IOException {
			Text next = findEdge(currentLabel);
			if (next != null) {
				boolean added = addEdge(next, targetLabel);
				if (added) {
					nudge(next);
				}
			}
		}

		private void constructCandidateChain(Text tag, Text src,
				Text candidateEdgeValue, Text reverseCandidateEdgeValue)
				throws IOException {
			Text targetId = findEdge(tag);
			if (targetId != null) {
				Text candidate = deriveEquivalentNode(src, targetId);
				addEdgeRequest(src, new Edge(candidate, candidateEdgeValue));
				addEdgeRequest(candidate, new Edge(src, reverseCandidateEdgeValue));
				nudge(new Text(src));
				nudge(new Text(candidate));
			}
		}

		private Text deriveEquivalentNode(Text vertexId, Text result) {
			String[] idSplit = vertexId.toString().split("/");
			String identifierFragment = idSplit[1];
			String candidate = String.format("%s/%s", result.toString(), identifierFragment );
			return new Text(candidate);
		}

		private void buildDiffLinks(Text vertexId, String myvalue, Text tag,
				String valueForComparison, Text srcVertex, Text reverseTag) throws IOException {
			if (valueForComparison.equals(myvalue)) {
				addEdgeRequest(srcVertex, new Edge(getId(), new Text("sameVersionForward")));
				addEdgeRequest(srcVertex, new Edge(getId(), new Text("p6")));
				addEdgeRequest(getId(), new Edge(srcVertex, new Text("sameVersionBackward")));
				addEdgeRequest(getId(), new Edge(srcVertex, new Text("r6")));
			} else {
				addEdgeRequest(srcVertex, new Edge(getId(), new Text("diffVersionForward")));
				addEdgeRequest(srcVertex, new Edge(getId(), new Text("p7")));
				addEdgeRequest(getId(), new Edge(srcVertex, new Text("diffVersionBackward")));
				addEdgeRequest(getId(), new Edge(srcVertex, new Text("r7")));
			}
			nudge(srcVertex);
			nudge(getId());
		}
		
		private void chainReduce(Text vertexId, String myvalue, Text tag,
				String valueForComparison, Text srcVertex, Text reverseTag) throws IOException {
			final Text result;
			if (!valueForComparison.equals(myvalue)) {
				result = vertexId;
			} else {
				result = findEdge(tag);
			}
			if (result != null) {
				addEdgeRequest(srcVertex, new Edge(result, tag));
				sendMessage(srcVertex, new Text("NOTIFY_CASCADE " + reverseTag.toString()));
			}
		}
		
		private Text findEdge(Text tag) {
			Text next = null;
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().equals(tag)) {
					next = e.getTargetVertexId();
				}
			}
			return next;
		}
		
		private void cascadeNudge(String cascadeTo) {
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals(cascadeTo)) { 
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

		private boolean isReduceCandidate() {
			boolean reduceCandidate = false;
			if (getId().toString().contains("X")) {
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
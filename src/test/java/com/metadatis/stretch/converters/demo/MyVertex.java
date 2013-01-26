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
		private final String candidateEdgeValue = "p5";
		private final String reverseCandidateEdgeValue = "r5";

		@Override
		public void compute(Iterable<Text> messages) throws IOException {
			
			String myvalue = calculateMyValue();
						
			for (Text m : messages) {
				String msg = m.toString();
				String[] split = msg.split(" ");
				String msgType = split[0];
				if (msgType.equals("NOTIFY_CASCADE")) {
					String cascadeTo = split[1];
					cascadeNudge(cascadeTo);
				} else if (msgType.equals("FIND_NEXT")) {
					String tag = split[1];
					String src = split[2];
					String candidateEdgeValue = split[3];
					String reverseCandidateEdgeValue = split[4];
					constructCandidateChain(new Text(tag), new Text(src), 
							new Text(candidateEdgeValue),
							new Text(reverseCandidateEdgeValue));
				} else if (msgType.equals("REDUCE")) {
					String tag = split[1];
					String valueForComparison = split[2];
					String src = split[3];
					String reverseTag = split[4];
					chainReduce(getId(), myvalue, new Text(tag), valueForComparison, 
							new Text(src), new Text(reverseTag));
				}
			}
			

			// Is this a reduce candidate
			boolean reduceCandidate = isReduceCandidate();
			String vertexId = getId().toString();
			
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

			voteToHalt();
			if (reduceCandidate && derived != null) {
				voteToHalt();
			} else if (reduceCandidate && candidate == null) {
				String[] split = vertexId.split("/");
				String derivedParent = split[0];
				Text message = new Text("FIND_NEXT p1 " + vertexId 
						+ " " + candidateEdgeValue + " " + reverseCandidateEdgeValue);
				sendMessage(new Text(derivedParent), message);
			} else {
				if (reduceCandidate && candidate != null) {
					Text message = new Text("REDUCE " 
							+ derivedEdgeValue
							+ " "
							+ myvalue + " " + vertexId
							+ " "
							+ reverseCandidateEdgeValue);
					sendMessage(new Text(candidate), message);
				}
			}
			voteToHalt();
		}

		private void constructCandidateChain(Text tag, Text src,
				Text candidateEdgeValue, Text reverseCandidateEdgeValue)
				throws IOException {
			Text targetId = findEdge(tag);
			if (targetId != null) {
				Text candidate = deriveEquivalentNode(src, targetId);
				addEdgeRequest(src, new Edge(candidate, candidateEdgeValue));
				addEdgeRequest(new Text(candidate), new Edge(new Text(src), reverseCandidateEdgeValue));
				nudge(new Text(src));
			}
		}

		private Text deriveEquivalentNode(Text vertexId, Text result) {
			String[] idSplit = vertexId.toString().split("/");
			String identifierFragment = idSplit[1];
			String candidate = String.format("%s/%s", result.toString(), identifierFragment );
			return new Text(candidate);
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
			sendMessage(target, new Text("_"));
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
package com.metadatis.stretch.converters.demo;

import java.io.IOException;
import java.util.Collections;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.HashMapVertex;
import org.apache.hadoop.io.NullWritable;
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
			// Is this a reduce candidate
			boolean reduceCandidate = isReduceCandidate();
			String vertexId = getId().toString();
			
			String myvalue = calculateMyValue();
						
			for (Text m : messages) {
				String msg = m.toString();
				if (msg.startsWith("NOTIFY_CASCADE ")) {
					String[] split = msg.split(" ");
					String cascadeTo = split[1];
					cascade(cascadeTo);
				} else if (msg.startsWith("FOUND ")) {
					handleFoundNext(vertexId, msg);
				} else if (msg.startsWith("FIND_NEXT ")) {
					handleFindNext(msg);
				} else if (msg.startsWith("REDUCE ")) {
					handleReduceMessage(vertexId, myvalue, msg);
				}
			}
			
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

		private void handleFindNext(String msg) throws IOException {
			String[] split = msg.split(" ");
			String tag = split[1];
			String src = split[2];
			String candidateEdgeValue = split[3];
			String reverseCandidateEdgeValue = split[4];
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals(tag)) {
					Text message = new Text("FOUND " + tag + " " + e.getTargetVertexId());
					String candidate = deriveEquivalentNode(src, e.getTargetVertexId().toString());
					addEdgeRequest(new Text(src), new Edge(new Text(candidate), new Text(candidateEdgeValue)));
					addEdgeRequest(new Text(candidate), new Edge(new Text(src), new Text(reverseCandidateEdgeValue)));
					sendMessage(new Text(src), message);
				}
			}
		}

		private void handleFoundNext(String vertexId, String msg)
				throws IOException {
//			String[] split = msg.split(" ");
//			String tag = split[1];
//			String result = split[2];
//			String candidate = deriveEquivalentNode(vertexId, result);
			//addEdge(new Text(candidate), new Text(candidateEdgeValue));
			//addEdgeRequest(new Text(candidate), new Edge(getId(), new Text(reverseCandidateEdgeValue)));
		}

		private String deriveEquivalentNode(String vertexId, String result) {
			String[] idSplit = vertexId.split("/");
			String identifierFragment = idSplit[1];
			String candidate = String.format("%s/%s", result, identifierFragment );
			return candidate;
		}

		private void handleReduceMessage(String vertexId, String myvalue,
				String msg) throws IOException {
			String[] split = msg.split(" ");
			String tag = split[1];
			String value = split[2];
			String src = split[3];
			String reverseTag = split[4];
			if (!value.equals(myvalue)) {
				rFound(vertexId, tag, src, reverseTag);
			} else {
				String next = findEdge(tag);
				if (next != null) {
					rFound(next, tag, src, reverseTag);
				}
			}
		}
		
		private String findEdge(String tag) {
			String next = null;
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals(tag)) {
					next = e.getTargetVertexId().toString();
				}
			}
			return next;
		}

		private void rFound(String vertexId, String tag, String src,
				String reverseTag) throws IOException {
			addEdgeRequest(new Text(src), new Edge(new Text(vertexId), new Text(tag)));
			sendMessage(new Text(src), new Text("NOTIFY_CASCADE " + reverseTag));
		}
		
		private void cascade(String cascadeTo) {
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
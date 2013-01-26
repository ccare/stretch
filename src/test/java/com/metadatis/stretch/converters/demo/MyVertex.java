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

		@Override
		public void compute(Iterable<Text> messages) throws IOException {
			String candidateEdgeValue = "p5";
			String reverseCandidateEdgeValue = "r5";
			String derivedEdgeValue = "p4";
			
			// Is this a reduce candidate
			boolean reduceCandidate = false;
			String vertexId = getId().toString();
			if (vertexId.contains("X")) {
				reduceCandidate = true;
			}
			
			String myvalue = null;
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals("p3")) {
					myvalue = e.getTargetVertexId().toString();
				}
			}
			
			
			for (Text m : messages) {
				String msg = m.toString();
				if (msg.startsWith("RFOUND ")) {
					String[] split = msg.split(" ");
					String dest = split[1];
					addEdge(new Text(dest), new Text(derivedEdgeValue));
					for (Edge<Text, Text> e : getEdges()) {
						if (e.getValue().toString().equals(reverseCandidateEdgeValue)) {
							sendMessage(e.getTargetVertexId(), new Text("_"));
						}
					}
				}
				if (msg.startsWith("FOUND ")) {
					String[] split = msg.split(" ");
					String tag = split[1];
					String result = split[2];
					String[] idSplit = vertexId.split("/");
					String identifierFragment = idSplit[1];
					String candidate = String.format("%s/%s", result, identifierFragment );
					addEdge(new Text(candidate), new Text(candidateEdgeValue));
					addEdgeRequest(new Text(candidate), new Edge(getId(), new Text(reverseCandidateEdgeValue)));
				} else if (msg.startsWith("FIND_NEXT ")) {
					String[] split = msg.split(" ");
					String tag = split[1];
					String src = split[2];
					for (Edge<Text, Text> e : getEdges()) {
						if (e.getValue().toString().equals(tag)) {
							Text message = new Text("FOUND " + tag + " " + e.getTargetVertexId());
							sendMessage(new Text(src), message);
						}
					}
				} else if (msg.startsWith("REDUCE ")) {
					String[] split = msg.split(" ");
					String value = split[1];
					String src = split[2];
					if (!value.equals(myvalue)) {
						Text message = new Text("RFOUND " + vertexId);
						sendMessage(new Text(src), message);
					} else {
						String next = null;
						for (Edge<Text, Text> e : getEdges()) {
							if (e.getValue().toString().equals(derivedEdgeValue)) {
								next = e.getTargetVertexId().toString();
							}
						}
						if (next != null) {
							Text message = new Text("RFOUND " + next);
							sendMessage(new Text(src), message);
						}
					}
				}
			}
			
			String candidate  = null;
			String derived  = null;
			for (Edge<Text, Text> e : getEdges()) {
				if (e.getValue().toString().equals(candidateEdgeValue)) {
					candidate = e.getTargetVertexId().toString();
				}
				if (e.getValue().toString().equals(derivedEdgeValue)) {
					derived = e.getTargetVertexId().toString();
				}
			}

			if (reduceCandidate && derived != null) {
				voteToHalt();
			} else if (reduceCandidate && candidate == null) {
				String[] split = vertexId.split("/");
				String derivedParent = split[0];
				Text message = new Text("FIND_NEXT p1 " + vertexId);
				sendMessage(new Text(derivedParent), message);
			} else if (reduceCandidate && candidate != null) {
				Text message = new Text("REDUCE " 
						+ myvalue + " " + vertexId);
				sendMessage(new Text(candidate), message);
			}
			voteToHalt();
			
			
			
			
			
//			// Answer any incoming messages
//			for (Text m : messages) {
//				String msg = m.toString();
//				System.out.println(msg);
//				if (msg.startsWith("FOUND ")) {
//					String[] split = msg.split(" ");
//					if (split[1].equals("p1")) {
//						sendMessage(new Text(split[2]), 
//								new Text("FIND p2 " + vertexId));
//					} else if (split[1].equals("p2")) {
//						addEdge(new Text(split[2]), new Text("p4"));
//					}
//					complete = true;
//				} else if (msg.startsWith("FIND ")) {
//					String[] split = msg.split(" ");
//					String type = split[1];
//					String requester = split[2];
//					for (Edge<Text, Text> e : getEdges()) {
//						if (e.getValue().toString().equals(type)) {
//							String resp = "FOUND " + type + " " + e.getTargetVertexId().toString();
//							sendMessage(new Text(requester), new Text(resp));
//						}
//					}
//				} else if (msg.startsWith("DIFFERENT ")) {
//					String[] split = msg.split(" ");
//					String type = split[1];
//					String value = split[2];
//					String requester = split[3];
//					for (Edge<Text, Text> e : getEdges()) {
//						if (e.getValue().toString().equals(type)) {
//							//if (!e.getTargetVertexId().toString().equals(value)) {
//								addEdgeRequest(new Text(requester), 
//										new Edge<Text, Text>(getId(), new Text("p4")));
//							//}
//						}
//					}
//				}
//			}
//			
//			// Manage
//			if (!reduceCandidate) {
//				voteToHalt();
//			} else if (complete) {
//				voteToHalt();
//			} else {
//				for (Edge<Text, Text> e : getEdges()) {
//					String eVal = e.getValue().toString();
//					if (eVal.equals("r2")) {
//						sendMessage(e.getTargetVertexId(), 
//								new Text("FIND p1 " + vertexId));
//					}
//				}
//				String myValue = null;
//				for (Edge<Text, Text> e : getEdges()) {
//					if (e.getValue().toString().equals("p3")) {
//						myValue = e.getTargetVertexId().toString();
//					}
//				}
//				if (myValue != null) {
//					for (Edge<Text, Text> e : getEdges()) {
//						String eVal = e.getValue().toString();
//						if (eVal.equals("p5")) {
//							sendMessage(e.getTargetVertexId(), 
//									new Text("DIFFERENT p3 " + myValue + " " + vertexId));
//						}
//					}
//				}
//				voteToHalt();
//			}
//			

//			if (myValue == null) {
//				voteToHalt();
//				return;
//			}
//			for (Text msg : messages) {				
//				String msgTxt = msg.toString();
//				if (msgTxt.startsWith("Result")) {
//					String[] split = msgTxt.split(" ");
//					String nextId = split[1];
//					setEdges(Collections.singletonMap(new Text(nextId), new Text("p4")));
//					foundNext = true;
//				} else {
//					String[] split = msgTxt.split(" ");
//					String src = split[0];
//					String srcValue = split[1];
//					if (! srcValue.equals(myValue)) {
//						sendMessage(new Text(src), new Text("Result " + getId().toString()));
//					} else if (foundNext) {
//						sendMessage(new Text(src), new Text("Result " + getEdges().iterator().next().getTargetVertexId()));
//					}					
//				}
//			}
//			Iterable<Edge<Text, Text>> edges = getEdges();
//			if (! edges.iterator().hasNext()) {
//				voteToHalt();
//			} else if (! foundNext) {
//				String msgText = String.format("%s %s", getId().toString(), myValue);
//				Text outboundMessage = new Text(msgText);
//				for (Edge<Text, Text> e : edges) {
//					System.out.println(String.format("%s -> %s", e.getTargetVertexId(), outboundMessage));
//					sendMessage(e.getTargetVertexId(), outboundMessage);
//				}
//			} else {
//				voteToHalt();
//			}
		}  	
  }
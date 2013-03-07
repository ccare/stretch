package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class CalculateForwardCandidateAction extends AbstractChainReduceAction implements MessageHandler {

		private final Text DUMMY_VERTEX = new Text("X");
		
		Text p1 = new Text("p1");
		Text r1 = new Text("r1");
		Text next = new Text("cNext");
		Text prev = new Text("cPrev");
		
		@Override
		public boolean triggerable(ChainReduceVertex vertex) {
			Text target = findEdgeByValue(vertex, next);
			return target == null;
		}

		@Override
		public void trigger(ChainReduceVertex vertex) throws IOException {
			String vertexId = vertex.getId().toString();
			String[] split = vertexId.split("/");
			String derivedParent = split[0];
			Text msg = new Text("FIND_NEXT " + vertexId);
			vertex.sendMessage(new Text(derivedParent), msg);
		}

		@Override
		public String getMessageType() {
			return "FIND_NEXT";
		}

		@Override
		public void handle(ChainReduceVertex vertex, String[] params) throws IOException {
			final Text src = new Text(params[1]);
			Text pTargetId = findEdgeByValue(vertex, p1);
			if (pTargetId != null) {
				Text candidate = deriveEquivalentNode(src, pTargetId);
				vertex.addEdgeRequest(src, (Edge<Text, Text>) new Edge(candidate, next));
				vertex.addEdgeRequest(candidate, (Edge<Text, Text>) new Edge(src, prev));
				nudge(vertex, new Text(candidate));
			} else {
				vertex.addEdgeRequest(src, (Edge<Text, Text>) new Edge(DUMMY_VERTEX, next));				
			}
			Text rTargetId = findEdgeByValue(vertex, r1);
			if (rTargetId != null) {
				Text candidate = deriveEquivalentNode(src, rTargetId);
				vertex.addEdgeRequest(src, (Edge<Text, Text>) new Edge(candidate, prev));
				vertex.addEdgeRequest(candidate, (Edge<Text, Text>) new Edge(src, next));
				nudge(vertex, new Text(candidate));
			} else {
				vertex.addEdgeRequest(src, (Edge<Text, Text>) new Edge(DUMMY_VERTEX, prev));				
			}
			nudge(vertex, new Text(src));
		}

		private Text deriveEquivalentNode(Text vertexId, Text result) {
			String[] idSplit = vertexId.toString().split("/");
			String identifierFragment = idSplit[1];
			String candidate = String.format("%s/%s", result.toString(),
					identifierFragment);
			return new Text(candidate);
		}

		@Override
		public boolean finished(ChainReduceVertex vertex) {
			Text target = findEdgeByValue(vertex, next);
			boolean foundTarget = target != null;
			boolean noSuchEdge = noEdge(vertex, next);
			return (foundTarget || noSuchEdge);
		}
		
		void nudge(ChainReduceVertex vertex, Text target) {
			if (target != null) {
				vertex.sendMessage(target, new Text("_"));
			}
		}
}
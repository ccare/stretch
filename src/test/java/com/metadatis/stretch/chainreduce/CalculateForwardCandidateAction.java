package com.metadatis.stretch.chainreduce;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

class CalculateForwardCandidateAction implements VertexAction,
			MessageHandler {

		/**
		 * 
		 */
		private final ChainReduceVertex vertex;

		/**
		 * @param chainReduceVertex
		 */
		CalculateForwardCandidateAction(ChainReduceVertex chainReduceVertex) {
			vertex = chainReduceVertex;
		}

		private final Text DUMMY_VERTEX = new Text("X");
		
		Text p1 = new Text("p1");
		Text r1 = new Text("r1");
		Text next = new Text("cNext");
		Text prev = new Text("cPrev");
		
		@Override
		public boolean triggerable() {
			Text target = vertex.findEdge(next);
			//boolean noSuchEdge = noEdge(next);
			return vertex.isReduceCandidate() && target == null;// && !noSuchEdge;
		}

		@Override
		public void trigger() throws IOException {
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
		public void handle(String[] params) throws IOException {
			final Text src = new Text(params[1]);
			Text pTargetId = vertex.findEdge(p1);
			if (pTargetId != null) {
				Text candidate = deriveEquivalentNode(src, pTargetId);
				vertex.sendEdgeRequest(src, new Edge(candidate, next));
				vertex.sendEdgeRequest(candidate, new Edge(src, prev));
				vertex.nudge(new Text(candidate));
			} else {
				vertex.sendEdgeRequest(src, new Edge(DUMMY_VERTEX, next));				
			}
			Text rTargetId = vertex.findEdge(r1);
			if (rTargetId != null) {
				Text candidate = deriveEquivalentNode(src, rTargetId);
				vertex.sendEdgeRequest(src, new Edge(candidate, prev));
				vertex.sendEdgeRequest(candidate, new Edge(src, next));
				vertex.nudge(new Text(candidate));
			} else {
				vertex.sendEdgeRequest(src, new Edge(DUMMY_VERTEX, prev));				
			}
			vertex.nudge(new Text(src));
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
			if (!vertex.isReduceCandidate()) {
				return true;
			}
			Text target = vertex.findEdge(next);
			boolean foundTarget = target != null;
			boolean noSuchEdge = vertex.noEdge(next);
//			if (getId().toString().equals("G/X")) {
//				return true;
//			}
			ChainReduceVertex.sb.append(String.format("%s: %s\n", vertex.getId(), foundTarget || noSuchEdge));
			return (foundTarget || noSuchEdge);
		}

		@Override
		public boolean applicable() {
			// TODO Auto-generated method stub
			return true;
		}
	}
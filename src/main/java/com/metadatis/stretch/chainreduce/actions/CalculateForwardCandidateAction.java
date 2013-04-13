package com.metadatis.stretch.chainreduce.actions;

import static com.metadatis.stretch.chainreduce.ChainReduceUtils.DUMMY_VERTEX;
import static com.metadatis.stretch.chainreduce.ChainReduceUtils.findEdgeByValue;
import static com.metadatis.stretch.chainreduce.ChainReduceUtils.noEdge;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;
import org.apache.log4j.varia.DenyAllFilter;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;
import com.metadatis.stretch.chainreduce.methods.DeriveEquivalent;
import com.metadatis.stretch.chainreduce.methods.VertexMethod;

public class CalculateForwardCandidateAction extends AbstractChainReduceAction implements MessageHandler<ChainReduceVertex> {
		
		private final String parentForwardLabelKey;
		private final String parentReverseLabelKey;
		private final String nextCandidateLabelKey;
		private final String prevCandidateLabelKey;
		private final String findParentMethodKey;
		private final String deriveEquivalentVertexKey;
		
		public CalculateForwardCandidateAction(final String p1, final String r1, 
				final String next, final String prev,
				final String findParentMethodKey,
				final String deriveEquivalentVertexKey) {
			this.parentForwardLabelKey = p1;
			this.parentReverseLabelKey = r1;
			this.nextCandidateLabelKey = next;
			this.prevCandidateLabelKey = prev;
			this.findParentMethodKey = findParentMethodKey;
			this.deriveEquivalentVertexKey = deriveEquivalentVertexKey;
		}

		@Override
		public boolean triggerable(ChainReduceVertex vertex) {
			final Text nextCandidateLabel = textFromConfig(vertex, nextCandidateLabelKey);
			return null == findEdgeByValue(vertex, nextCandidateLabel);
		}

		@Override
		public void trigger(ChainReduceVertex vertex) throws IOException {
			VertexMethod<String> m = methodFromConfig(vertex, findParentMethodKey, String.class);
			String derivedParent = m.calculate(vertex);
			Text msg = new Text("FIND_NEXT " + vertex.getId().toString());
			vertex.sendMessage(new Text(derivedParent), msg);
		}

		@Override
		public String getMessageType() {
			return "FIND_NEXT";
		}

		@Override
		public void handle(ChainReduceVertex vertex, String[] params) throws IOException {
			final Text parentForwardLabel = textFromConfig(vertex, parentForwardLabelKey);
			final Text parentReverseLabel = textFromConfig(vertex, parentReverseLabelKey);
			final Text nextCandidateLabel = textFromConfig(vertex, nextCandidateLabelKey);
			final Text prevCandidateLabel = textFromConfig(vertex, prevCandidateLabelKey);
			final Text src = new Text(params[1]);
			Text pTargetId = findEdgeByValue(vertex, parentForwardLabel);
			if (pTargetId != null) {
				Text candidate = methodFromConfig(vertex, deriveEquivalentVertexKey, Text.class).calculate(vertex, src, pTargetId);
				vertex.addEdgeRequest(src, new Edge<Text, Text>(candidate, nextCandidateLabel));
				vertex.addEdgeRequest(candidate, new Edge<Text, Text>(src, prevCandidateLabel));
				nudge(vertex, new Text(candidate));
			} else {
				vertex.addEdgeRequest(src, new Edge<Text, Text>(DUMMY_VERTEX, nextCandidateLabel));				
			}
			Text rTargetId = findEdgeByValue(vertex, parentReverseLabel);
			if (rTargetId != null) {
				Text candidate = methodFromConfig(vertex, deriveEquivalentVertexKey, Text.class).calculate(vertex, src, rTargetId);
				vertex.addEdgeRequest(src, new Edge<Text, Text>(candidate, prevCandidateLabel));
				vertex.addEdgeRequest(candidate, new Edge<Text, Text>(src, nextCandidateLabel));
				nudge(vertex, new Text(candidate));
			} else {
				vertex.addEdgeRequest(src, new Edge<Text, Text>(DUMMY_VERTEX, prevCandidateLabel));				
			}
			nudge(vertex, new Text(src));
		}

		@Override
		public boolean finished(ChainReduceVertex vertex) {
			final Text nextCandidateLabel = textFromConfig(vertex, nextCandidateLabelKey);
			Text target = findEdgeByValue(vertex, nextCandidateLabel);
			boolean foundTarget = target != null;
			boolean noSuchEdge = noEdge(vertex, nextCandidateLabel);
			return (foundTarget || noSuchEdge);
		}
		
		void nudge(ChainReduceVertex vertex, Text target) {
			if (target != null) {
				vertex.sendMessage(target, new Text("_"));
			}
		}
}
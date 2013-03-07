package com.metadatis.stretch.chainreduce;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

class DiffSameAction implements VertexAction,
MessageHandler {
	
	/**
	 * 
	 */
	private final ChainReduceVertex vertex;
	private final CalculateForwardCandidateAction candidateAction;
	private final Text sameLabel;
	private final Text diffLabel;
	private final Text candidateLabel;
	
	boolean done = false;
	
	public DiffSameAction(ChainReduceVertex chainReduceVertex, Text sameLabel, Text diffLabel, final Text candidateLabel, CalculateForwardCandidateAction candidateAction) {
		vertex = chainReduceVertex;
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
		
		String calculateMyValue = vertex.calculateMyValue();
		
		Text sourceVertexId = new Text(src);
		if (calculateMyValue.equals(value)) {
			vertex.sendEdgeRequest(sourceVertexId, new Edge(vertex.getId(), sameLabel));
		} else {
			vertex.sendEdgeRequest(sourceVertexId, new Edge(vertex.getId(),  diffLabel));				
		}			
	}

	@Override
	public boolean triggerable() {
		if (! vertex.isReduceCandidate()) {
			return false;
		} else if (!candidateAction.finished()) {
			return false;
		} else if (null == vertex.findEdge(candidateLabel)) {
			return false;
		} else if (vertex.noEdge(candidateLabel)) {
			return false;
		}  else {
			return true;
		}
	}

	@Override
	public boolean finished() {
		if (!vertex.isReduceCandidate()) {
			return true;
		} else if (!candidateAction.finished()) {
			return false;
		} else {
			return null != vertex.findEdge(sameLabel) || null != vertex.findEdge(diffLabel);
		}
	}

	@Override
	public void trigger() throws IOException {
		String vertexId = vertex.getId().toString();
		Text candidate = vertex.findEdge(candidateLabel);
		Text msg = new Text(getMessageType() + " " + vertexId + " " + vertex.calculateMyValue());
		vertex.sendMessage(candidate, msg);
		done = true;
	}

	@Override
	public boolean applicable() {
		// TODO Auto-generated method stub
		return true;
	}
	
}
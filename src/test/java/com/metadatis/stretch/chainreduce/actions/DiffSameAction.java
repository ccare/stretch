package com.metadatis.stretch.chainreduce.actions;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class DiffSameAction extends AbstractChainReduceAction implements MessageHandler {
	
	private final CalculateForwardCandidateAction candidateAction;
	private final Text sameLabel;
	private final Text diffLabel;
	private final Text candidateLabel;
	
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
	public void handle(ChainReduceVertex vertex, String[] params) throws IOException {
		String src = params[1];
		String value = params[2];
		
		String calculateMyValue = calculateVertexValue(vertex);
		
		Text sourceVertexId = new Text(src);
		if (calculateMyValue.equals(value)) {
			vertex.addEdgeRequest(sourceVertexId, (Edge<Text, Text>) new Edge(vertex.getId(), sameLabel));
		} else {
			vertex.addEdgeRequest(sourceVertexId, (Edge<Text, Text>) new Edge(vertex.getId(),  diffLabel));				
		}			
	}

	@Override
	public boolean triggerable(ChainReduceVertex vertex) {
		if (!candidateAction.finished(vertex)) {
			return false;
		} else if (null == findEdgeByValue(vertex, candidateLabel)) {
			return false;
		} else if (noEdge(vertex, candidateLabel)) {
			return false;
		}  else {
			return true;
		}
	}


	private String calculateVertexValue(ChainReduceVertex vertex) {
		String myvalue = null;
		for (Edge<Text, Text> e : vertex.getEdges()) {
			if (e.getValue().toString().equals("p3")) {
				myvalue = e.getTargetVertexId().toString();
			}
		}
		return myvalue;
	}

	@Override
	public boolean finished(ChainReduceVertex vertex) {
		if (!candidateAction.finished(vertex)) {
			return false;
		} else if (null == findEdgeByValue(vertex, candidateLabel)) {
			return true;
		} else {
			Text sameLink = findEdgeByValue(vertex, sameLabel);
			Text diffLink = findEdgeByValue(vertex, diffLabel);
			if (null == sameLink && null == diffLink) {
				return false;
			} else {
				return true;
			}
		}
	}

	@Override
	public void trigger(ChainReduceVertex vertex) throws IOException {
		String vertexId = vertex.getId().toString();
		Text candidate = findEdgeByValue(vertex, candidateLabel);
		Text msg = new Text(getMessageType() + " " + vertexId + " " + calculateVertexValue(vertex));
		vertex.sendMessage(candidate, msg);
	}
	
}
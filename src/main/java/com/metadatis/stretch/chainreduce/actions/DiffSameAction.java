package com.metadatis.stretch.chainreduce.actions;

import static com.metadatis.stretch.chainreduce.ChainReduceUtils.findEdgeByValue;
import static com.metadatis.stretch.chainreduce.ChainReduceUtils.noEdge;

import java.io.IOException;

import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class DiffSameAction extends AbstractChainReduceAction implements MessageHandler<ChainReduceVertex> {
	
	private final CalculateForwardCandidateAction candidateAction;
	private final String sameLabelKey;
	private final String diffLabelKey;
	private final String candidateLabelKey;
	
	public DiffSameAction(String sameLabelKey, String diffLabelKey, final String candidateLabelKey, 
			final CalculateForwardCandidateAction candidateAction) {
		this.sameLabelKey = sameLabelKey;
		this.diffLabelKey = diffLabelKey;
		this.candidateLabelKey = candidateLabelKey;
		this.candidateAction = candidateAction;
	}

	@Override
	public String getMessageType() {
		return "DIFF_SAME_" + candidateLabelKey;
	}

	@Override
	public void handle(ChainReduceVertex vertex, String[] params) throws IOException {
		String src = params[1];
		String value = params[2];
		
		String calculateMyValue = calculateVertexValue(vertex);
		
		Text sourceVertexId = new Text(src);
		if (calculateMyValue.equals(value)) {
			Text sameLabel = textFromConfig(vertex, sameLabelKey);
			vertex.addEdgeRequest(sourceVertexId, new Edge<Text, Text>(vertex.getId(), sameLabel));
		} else {
			Text diffLabel = textFromConfig(vertex, diffLabelKey);
			vertex.addEdgeRequest(sourceVertexId, new Edge<Text, Text>(vertex.getId(),  diffLabel));				
		}			
	}

	@Override
	public boolean triggerable(ChainReduceVertex vertex) {
		Text candidateLabel = textFromConfig(vertex, candidateLabelKey);
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
		Text candidateLabel = textFromConfig(vertex, candidateLabelKey);
		if (!candidateAction.finished(vertex)) {
			return false;
		} else if (null == findEdgeByValue(vertex, candidateLabel)) {
			return true;
		} else {
			Text sameLabel = textFromConfig(vertex, sameLabelKey);
			Text diffLabel = textFromConfig(vertex, diffLabelKey);
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
		Text candidateLabel = textFromConfig(vertex, candidateLabelKey);
		Text candidate = findEdgeByValue(vertex, candidateLabel);
		Text msg = new Text(getMessageType() + " " + vertexId + " " + calculateVertexValue(vertex));
		vertex.sendMessage(candidate, msg);
	}
	
}
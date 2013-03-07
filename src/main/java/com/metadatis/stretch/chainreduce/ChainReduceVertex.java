package com.metadatis.stretch.chainreduce;


import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.actions.CalculateForwardCandidateAction;
import com.metadatis.stretch.chainreduce.actions.DiffSameAction;
import com.metadatis.stretch.chainreduce.actions.ReverseEdgeAction;

public class ChainReduceVertex extends AbstractActionBasedComputationVertex {

	private static final Text P1 = new Text("p1");
	private static final Text R1 = new Text("r1");

	private static final Text P2 = new Text("p2");
	private static final Text R2 = new Text("r2");

	private static final Text CANDIDATE_NEXT = new Text("cNext");
	private static final Text CANDIDATE_PREV = new Text("cPrev");

	private static final Text FORWARD_SAME_LABEL = new Text("fSame");
	private static final Text FORWARD_DIFFERENT_LABEL = new Text("fDiff");		
	
	private static final Text BACKWARD_SAME_LABEL = new Text("bSame");
	private static final Text BACKWARD_DIFFERENT_LABEL = new Text("bDiff");	
	
	static {		
		CalculateForwardCandidateAction createCandidateAction = new CalculateForwardCandidateAction(P1, R1, CANDIDATE_NEXT, CANDIDATE_PREV);
		
		registry.register(new ReverseEdgeAction(P1, R1));
		registry.register(new ReverseEdgeAction(P2, R2));
		registry.register(createCandidateAction);
		registry.register(new DiffSameAction(FORWARD_SAME_LABEL, FORWARD_DIFFERENT_LABEL, CANDIDATE_NEXT, createCandidateAction));
		registry.register(new DiffSameAction(BACKWARD_SAME_LABEL, BACKWARD_DIFFERENT_LABEL, CANDIDATE_PREV, createCandidateAction));
	}

}
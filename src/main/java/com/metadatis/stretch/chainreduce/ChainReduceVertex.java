package com.metadatis.stretch.chainreduce;


import java.util.concurrent.ExecutionException;

import org.apache.hadoop.io.Text;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.metadatis.stretch.chainreduce.actions.AbstractChainReduceAction;
import com.metadatis.stretch.chainreduce.actions.CalculateForwardCandidateAction;
import com.metadatis.stretch.chainreduce.actions.DiffSameAction;
import com.metadatis.stretch.chainreduce.actions.ReverseEdgeAction;
import com.metadatis.stretch.chainreduce.methods.VertexMethod;

public class ChainReduceVertex extends AbstractActionBasedComputationVertex {

	public static final String P1_KEY = "p1";
	public static final String R1_KEY = "r1";
	public static final String P2_KEY = "p2";
	public static final String R2_KEY = "r2";
	public static final String CANDIDATE_NEXT_KEY = "cNext";
	public static final String CANDIDATE_PREV_KEY = "cPrev";
	public static final String FORWARD_SAME_LABEL_KEY = "fSame";
	public static final String FORWARD_DIFFERENT_LABEL_KEY = "fDiff";
	public static final String BACKWARD_SAME_LABEL_KEY = "bSame";
	public static final String BACKWARD_DIFFERENT_LABEL_KEY = "bDiff";
	public static final String APPLICABLE_CLASS = "applicable-class";
	
	private static final LoadingCache<String, VertexMethod> methods;
	
	static {		
		methods = CacheBuilder.newBuilder()
			       .maximumSize(10000)
			       .build(
			           new CacheLoader<String, VertexMethod>() {
			             public VertexMethod load(String className) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
			            	 Class<VertexMethod> clazz = (Class<VertexMethod>) AbstractChainReduceAction.class.getClassLoader().loadClass(className);
			               return clazz.newInstance();
			             }
			           });		
	}
	
	public <T> VertexMethod<T> getMethod(String className, Class<T> clazz) {
		try {
			return methods.get(className);
		} catch (ExecutionException e) {
			throw new RuntimeException("BOOM", e);
		}
	}
	
	static {		
		CalculateForwardCandidateAction createCandidateAction = new CalculateForwardCandidateAction(P1_KEY, R1_KEY, CANDIDATE_NEXT_KEY, CANDIDATE_PREV_KEY);
		
		registry.register(new ReverseEdgeAction(P1_KEY, R1_KEY));
		registry.register(new ReverseEdgeAction(P2_KEY, R2_KEY));
		registry.register(createCandidateAction);
		registry.register(new DiffSameAction(FORWARD_SAME_LABEL_KEY, FORWARD_DIFFERENT_LABEL_KEY, CANDIDATE_NEXT_KEY, createCandidateAction));
		registry.register(new DiffSameAction(BACKWARD_SAME_LABEL_KEY, BACKWARD_DIFFERENT_LABEL_KEY, CANDIDATE_PREV_KEY, createCandidateAction));
	}

}
package com.metadatis.stretch.chainreduce;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static com.metadatis.stretch.chainreduce.ChainReduceVertex.*;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class ChainReduceTest {

	
		  /*
		   *   A -- B -- C -- D -- E -- F -- G    p1
		   *   |    |    |    |    |    |    |    p2
		   *  A/X  B/X  C/X  D/X  E/X  F/X  G/X
		   *   |    |    |    |    |    |    |    p3
		   *   a    a    b    b    c    c    d
		   *   
		   *  A/X ----> C/X ----> E/X ----> G/X   p4  
		   *      B/X > C/X                       p4
		   *                D/X > E/X             p4
		   *                          F/X > G/X   p4       
		   *                          
		   *  A/X <---- C/X <---- E/X <---- G/X   p5  
		   *  A/X <---------- D/X                 p5
		   *            C/X <--------  F/X        p5 
		   *            
		   *  A/X > B/X                           p6 (forwardSame)
		   *           C/X > D/X                  p6
		   *                     E/X > F/X        p6
		   *                     
		   *       B/X > C/X                      p7 (forwardDifferent)
		   *                 D/X > E/X            p7
		   *                           F/X > G/X  p7    
		   *                           
		   *                             
		   *                             
		   *                     
		   */

	
	String[] tinyGraph =
	        new String[] { 
	    		"A p1 B;A p2 A/X", 
	    		"B p2 B/X", 
	    		"A/X p3 a",
	    		"B/X p3 a",
	    		"a _ _",
	    		"b _ _"
	    		};
	

	String[] smallGraph =
	        new String[] { 
	    		"A p1 B;A p2 A/X", 
	    		"B p1 C;B p2 B/X", 
	    		"C p2 C/X", 
	    		"A/X p3 a",
	    		"B/X p3 a",
	    		"C/X p3 b",
	    		"a _ _",
	    		"b _ _",
	    		"c _ _"
	    		};

	String[] graph =
	        new String[] { 
	    		"A p1 B;A p2 A/X", 
	    		"B p1 C;B p2 B/X", 
	    		"C p1 D;C p2 C/X", 
	    		"D p1 E;D p2 D/X", 
	    		"E p1 F;E p2 E/X", 
	    		"F p1 G;F p2 F/X",
	    		"G p2 G/X",
	    		"A/X p3 a",
	    		"B/X p3 a",
	    		"C/X p3 b",
	    		"D/X p3 b",
	    		"E/X p3 c",
	    		"F/X p3 c",
	    		"G/X p3 d",
	    		"a _ _",
	    		"b _ _",
	    		"c _ _",
	    		"d _ _",
	    		"e _ _",
	    		"f _ _",
	    		"X _ _"};	
	
	private Map<String, String> params;
	
	@Before
	public void setup() {
		params = Maps.newHashMap();
		applyDefaultConfig(params);
	}
	
	@Test
	public void canOutputEdgesOfAGivenType() throws Exception {
		   
		    String[] expectedResults =
		        new String[] { 
		    		"A p1 B", 
		    		"B p1 C", 
		    		"C p1 D", 
		    		"D p1 E", 
		    		"E p1 F", 
		    		"F p1 G" };
		    
		    
		    params.put("exportEdges", "p1");

		    Collection<String> results = process(params);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}


	private Collection<String> process(Map<String, String> params)
			throws Exception {
		Collection<String> results = process(graph, params);

	    System.out.println(results);
	    
		return results;
	}
	
	@Test
	public void canOutputIntermediateR1Edges() throws Exception {
		   
		    String[] expectedResults =
		        new String[] { 
		    		"B r1 A", 
		    		"C r1 B", 
		    		"D r1 C", 
		    		"E r1 D", 
		    		"F r1 E", 
		    		"G r1 F" };
		    
		    
		    params.put("exportEdges", "r1");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}

	
	@Test
	public void canOutputIntermediateR2Edges() throws Exception {
		   
		    String[] expectedResults =
		        new String[] { 
		    		"A/X r2 A", 
		    		"B/X r2 B", 
		    		"C/X r2 C", 
		    		"D/X r2 D", 
		    		"E/X r2 E", 
		    		"F/X r2 F", 
		    		"G/X r2 G" };
		    
		    
		    params.put("exportEdges", "r2");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidateNext() throws Exception {
		   
		    String[] expectedResults =
		        new String[] {
		    		"A/X cNext B/X", 
		    		"B/X cNext C/X", 
		    		"C/X cNext D/X", 
		    		"D/X cNext E/X", 
		    		"E/X cNext F/X", 
		    		"F/X cNext G/X", 
		    		"G/X cNext X" };
		    
		    
		    params.put("exportEdges", "cNext");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidatePrevious() throws Exception {
		   
		    String[] expectedResults =
		        new String[] {
		    		"A/X cPrev X",
		    		"B/X cPrev A/X", 
		    		"C/X cPrev B/X", 
		    		"D/X cPrev C/X", 
		    		"E/X cPrev D/X", 
		    		"F/X cPrev E/X", 
		    		"G/X cPrev F/X"  };
		    
		    
		    params.put("exportEdges", "cPrev");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidateNextForTinyGraph() throws Exception {
		   
		    String[] expectedResults =
		        new String[] { 
		    		"A/X cNext B/X",
		    		"B/X cNext X",
		    		"A/X cPrev X",
		    		"B/X cPrev A/X",
		    		};
		    
		    
		    params.put("exportEdges", "cNext,cPrev");

		    Collection<String> results = process(tinyGraph, params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidateBackwardSame() throws Exception {
		    String[] expectedResults =
		        new String[] {
		    		"B/X bSame A/X", 
		    		"D/X bSame C/X", 
		    		"F/X bSame E/X"   };
		    
		    
		    params.put("exportEdges", "bSame");

		    Collection<String> results = process(params);
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));		    
	}
	
	@Test
	public void canOutputCandidateBackwardDiff() throws Exception {
		String[] expectedResults =
	        new String[] {
	    		"C/X bDiff B/X", 
	    		"E/X bDiff D/X", 
	    		"G/X bDiff F/X" };
	    
	    
	    params.put("exportEdges", "bDiff");

	    Collection<String> results = process(params);

	    System.out.println(results);
	    
	    List<String> expected = asList(expectedResults);
		Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidateForwardDiff() throws Exception {		   
		    String[] expectedResults =
		        new String[] {
		    		"B/X fDiff C/X", 
		    		"D/X fDiff E/X", 
		    		"F/X fDiff G/X"   };
		    
		    
		    params.put("exportEdges", "fDiff,fSame");

		    Collection<String> results = process(params);
		    
		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}
	
	@Test
	public void canOutputCandidateForwardSame() throws Exception {
		    String[] expectedResults =
		        new String[] {
		    		"A/X fSame B/X", 
		    		"C/X fSame D/X", 
		    		"E/X fSame F/X"   };
		    
		    
		    params.put("exportEdges", "fSame");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}

	@Ignore
	@Test
	public void canOutputP4() throws Exception {		   
		    String[] expectedResults =
		        new String[] { 
		    		"A/X p4 C/X", 
		    		"B/X p4 C/X", 
		    		"C/X p4 E/X", 
		    		"D/X p4 E/X", 
		    		"E/X p4 G/X", 
		    		"F/X p4 G/X" };
		    
		    
		    params.put("exportEdges", "p4");

		    Collection<String> results = process(params);

		    System.out.println(results);
		    
		    List<String> expected = asList(expectedResults);
			Assert.assertTrue(results.containsAll(expected));
	}

	private Collection<String> process(String[] graph,
			Map<String, String> myParams) throws Exception {
	    final Class<ChainReduceVertex> vertexClass = ChainReduceVertex.class;
		Collection<String> results = new ArrayList<String>();
		for (String r :
	        InternalVertexRunner.run(vertexClass, null,
	        		SimpleInputFormat.class,
	            SimpleOutputFormat.class,
	            null, null,
	            myParams, graph)) {
	    	results.add(r);
	    }
		return results;
	}
	
	public static void applyDefaultConfig(Map<String, String> config) {
		config.put(P1_KEY, "p1");
		config.put(R1_KEY, "r1");
		config.put(P2_KEY, "p2");
		config.put(R2_KEY, "r2");
		config.put(CANDIDATE_NEXT_KEY, "cNext");
		config.put(CANDIDATE_PREV_KEY, "cPrev");
		config.put(FORWARD_SAME_LABEL_KEY, "fSame");
		config.put(FORWARD_DIFFERENT_LABEL_KEY, "fDiff");
		config.put(BACKWARD_SAME_LABEL_KEY, "bSame");
		config.put(BACKWARD_DIFFERENT_LABEL_KEY, "bDiff");
		config.put(APPLICABLE_CLASS, "com.metadatis.stretch.chainreduce.methods.IsApplicable");
		config.put("find-parent", "com.metadatis.stretch.chainreduce.methods.DeriveParent");
	}
	  

}

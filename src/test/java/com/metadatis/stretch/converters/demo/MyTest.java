package com.metadatis.stretch.converters.demo;

import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class MyTest {

	@Test
	  public void testGraph() throws Exception {
		  /*
		   *   A - B - C - D - E - F - G
		   *   a   a   b   b   c   c   d
		   * 
		   *   A ----> C ----> E ----> G
		   */
		  
	    // Input
	    String[] graph =
	        new String[] { 
	    		"A(a) B", 
	    		"B(a) C", 
	    		"C(b) D", 
	    		"D(b) E", 
	    		"E(c) F", 
	    		"F(c) G", 
	    		"G(d) _" };
	    
	    String[] expectedResults =
	        new String[] { 
	    		"A(a) C", 
	    		"B(a) C", 
	    		"C(b) E", 
	    		"D(b) E", 
	    		"E(c) G", 
	    		"F(c) G", 
	    		"G(d) _" };
	    
	    Map<String, String> myParams = Maps.newHashMap();

	    Collection<String> results = process(graph, myParams);
	    	    
	    List<String> expected = asList(expectedResults);
		Assert.assertTrue(results.containsAll(expected));
	  }
	

	@Test
	  public void testGraphWithTraversal() throws Exception {
		  /*
		   *   A -- B -- C -- D -- E -- F -- G    p1
		   *   |    |    |    |    |    |    |    p2,r2
		   *  A/X  B/X  C/X  D/X  E/X  F/X  G/X
		   *   |    |    |    |    |    |    |    p3
		   *   a    a    b    b    c    c    d
		   *   
		   *  A/X ----> C/X ----> E/X ----> G/X   p4  
		   *      B/X > C/X                       p4
		   *                D/X > E/X             p4
		   *                          F/X > G/X   p4       
		   *                          
		   *  A/X <---- C/X <---- E/X <---- G/X   r4  
		   *  A/X < B/X                           r4
		   *            C/X < D/X                 r4
		   *                      E/X < F/X       r4 
		   *       
		   */
		  
	    // Input
	    String[] graph =
	        new String[] { 
	    		"A p1 B;A p2 A/X", 
	    		"B p1 C;B p2 B/X", 
	    		"C p1 D;C p2 C/X", 
	    		"D p1 E;D p2 D/X", 
	    		"E p1 F;E p2 E/X", 
	    		"F p1 G;F p2 F/X",
	    		"G p2 G/X7",
	    		"A/X p3 a;A/X r2 A",
	    		"B/X p3 a;B/X r2 B",
	    		"C/X p3 b;C/X r2 C",
	    		"D/X p3 b;D/X r2 D",
	    		"E/X p3 c;E/X r2 E",
	    		"F/X p3 c;F/X r2 F",
	    		"G/X p3 d;G/X r2 G",
	    		"a _ _",
	    		"b _ _",
	    		"c _ _",
	    		"d _ _",
	    		"e _ _",
	    		"f _ _"};
	   
	    String[] minimalExpectedResults =
	        new String[] { 
	    		"A/X1 p4 C/X3", 
	    		"C/X3 p4 E/X5", 
	    		"E/X5 p4 G/X7" };
	    
	    String nextItemPred = "p4";
	    String prevItemPred = "r4";
	    
	    String[] expectedResults =
	        new String[] { 
	    		"A/X p4 C/X", 
	    		"B/X p4 C/X", 
	    		"C/X p4 E/X", 
	    		"D/X p4 E/X", 
	    		"E/X p4 G/X", 
	    		"F/X p4 G/X",
	    		// & reverse
//	    		"G/X r4 E/X", 
//	    		"F/X r4 E/X", 
//	    		"E/X r4 C/X", 
//	    		"D/X r4 C/X", 
//	    		"C/X r4 A/X", 
//	    		"B/X r4 A/X" 
	    		};
	    
	    Map<String, String> myParams = Maps.newHashMap();

	    Collection<String> results = process(graph, myParams);

	    System.out.println(results);
	    
	    List<String> expected = asList(expectedResults);
		Assert.assertTrue(results.containsAll(expected));
	  }

	private Collection<String> process(String[] graph,
			Map<String, String> myParams) throws Exception {
		Collection<String> results = new ArrayList<String>();
	    for (String r :
	        InternalVertexRunner.run(MyVertex.class, null,
	        		SimpleInputFormat.class,
	            SimpleOutputFormat.class,
	            null, null,
	            myParams, graph)) {
	    	results.add(r);
	    }
		return results;
	}
	  

}

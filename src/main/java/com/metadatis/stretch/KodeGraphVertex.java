package com.metadatis.stretch;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.giraph.graph.HashMapVertex;
import org.apache.hadoop.io.Text;

import com.metadatis.stretch.messages.GraphMessage;
import com.metadatis.stretch.messages.GraphMessage.Type;


public class KodeGraphVertex extends HashMapVertex<Text, Text, Text, GraphMessage>  {
	
	static RelationshipComputation util = new RelationshipComputation();
	
	static Map<Text, RelationshipComputation> utils = Collections.singletonMap(util.getRelationship(), util);

	private Map<String, String> answerRepository = new HashMap<String, String>();
	
	@Override
	public void compute(Iterable<GraphMessage> messages) throws IOException {
		for (GraphMessage message : messages) {
			Type type = message.getType();
			Text relationshipType = message.getRelationshipType();
			RelationshipComputation command = utils.get(relationshipType);
			if (type == Type.FOUND) {
				command.handleAnswer(message, this);
			} else if (type == Type.FIND) {
				if (command != null 
						&& command.amINextNeighbour(this, message.getValue())) {
					Text src = message.getVertex();
					GraphMessage response = new GraphMessage();
					response.setType(Type.FOUND);
					response.setRelationshipType(relationshipType);
					response.setVertex(getId());
					sendMessage(src, response);
				} else {
					Text vertexId = findAdjacentVertexByEdgeValue(relationshipType);
					if (vertexId != null) {
						Text src = message.getVertex();
						GraphMessage response = new GraphMessage();
						response.setType(Type.FOUND);
						response.setRelationshipType(relationshipType);
						response.setVertex(vertexId);
						sendMessage(src, response);
					} 				
				}
			}
		}	
		boolean questionsOutstanding = false;
		for (RelationshipComputation u : utils.values()) {			
			Text candidate = u.getCandidate(getId());
			if (u.isApplicable(this)) {
				Text answer = findAdjacentVertexByEdgeValue(u.getRelationship());
				if (answer == null) {
					questionsOutstanding = true;		
					if (! u.isWaitingOnInformation(this)) {		
						GraphMessage request = new GraphMessage();
						request.setType(Type.FIND);
						request.setVertex(getId());
						request.setValue(u.getComparableValue(this));
						sendMessage(candidate, request);
					}
				}
			}
		}
		if (!questionsOutstanding) {
			voteToHalt();
		}
	}

	private Text findAdjacentVertexByEdgeValue(Text relationshipType) {
		// TODO Auto-generated method stub
		return null;
	}
	
}

abstract class FindCommit implements IComputation {

	@Override
	public Text getRelationship() {
		return new Text("PARENT_COMMIT");
	}

	@Override
	public boolean isApplicable(KodeGraphVertex vertex) {
		return vertex.getId().toString().startsWith("http://kodegraf.metadatis.com/items/h1/git-paths/");
	}

	@Override
	public boolean isWaitingOnInformation(KodeGraphVertex vertex) {
		return false;
	}

	@Override
	public Text getCandidate(Text vertexId) {
		String v = vertexId.toString();
		Pattern p = Pattern.compile(".*git-paths\\/([^/]+).*");
		Matcher matcher = p.matcher(v);
		String commit = matcher.group(1);
		return new Text("http://kodegraf.metadatis.com/items/h1/git-objects/" + commit);
	}

	
}

/**
 * Represents the parameters of a relationship determining algorithm.
 * 
 * If applicable, enables a vertex to walk through a series of candidate 
 * vertices looking for one that could fit
 */
class RelationshipComputation {
	
	public Text getRelationship() {
		return new Text();
	}

	public void handleAnswer(GraphMessage message,
			KodeGraphVertex vertex)
	{
		vertex.addEdge(message.getVertex(), message.getRelationshipType());	
	}

	/**
	 * Does this vertex need an answer to the given question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation applies and should be run, false if it
	 * should be ignored
	 */
	public boolean isApplicable(KodeGraphVertex vertex) {
		return true;
	}

	/**
	 * Does this vertex need more information populated before it can answer 
	 * the question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation should be postponed, false otherwise
	 */
	public boolean isWaitingOnInformation(KodeGraphVertex vertex) {
		return false;
	}
	
	/**
	 * The value that will be sent to a candidate result 
	 * 
	 * @param vertex a reference to the source vertex
	 * @return the value
	 */
	public Text getComparableValue(KodeGraphVertex vertex) {
		return new Text();
	}

	/**
	 * Find the candidate vertex which may be the answer 
	 * 
	 * @param vertexId The id of the current vertex
	 * @return The vertexId of the next candidate answer which a message
	 * will be sent to
	 */
	public Text getCandidate(Text vertexId) {
		return null;
	}
	
	/**
	 * Given an incoming value, determine whether this vertex could be
	 * the next neighbour that the computation is looking for.
	 * 
	 * @param currentVertex the vertex which will be compared
	 * @param otherValue the value which the requesting vertex sent
	 * 
	 * @return true if the currentVertex can be considered a result
	 */
	public boolean amINextNeighbour(KodeGraphVertex currentVertex, Text otherValue) {
		return getComparableValue(currentVertex).equals(otherValue);
	}
}




interface IComputation {
	
	public Text getRelationship();

	/**
	 * Does this vertex need an answer to the given question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation applies and should be run, false if it
	 * should be ignored
	 */
	public boolean isApplicable(KodeGraphVertex vertex);

	/**
	 * Does this vertex need more information populated before it can answer 
	 * the question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation should be postponed, false otherwise
	 */
	public boolean isWaitingOnInformation(KodeGraphVertex vertex);
	
	/**
	 * The value that will be sent to a candidate result 
	 * 
	 * @param vertex a reference to the source vertex
	 * @return the value
	 */
	public Text getComparableValue(KodeGraphVertex vertex);

	/**
	 * Find the candidate vertex which may be the answer 
	 * 
	 * @param vertexId The id of the current vertex
	 * @return The vertexId of the next candidate answer which a message
	 * will be sent to
	 */
	public Text getCandidate(Text vertexId);
	
	/**
	 * Given an incoming value, determine whether this vertex could be
	 * the next neighbour that the computation is looking for.
	 * 
	 * @param currentVertex the vertex which will be compared
	 * @param otherValue the value which the requesting vertex sent
	 * 
	 * @return true if the currentVertex can be considered a result
	 */
	public boolean amINextNeighbour(KodeGraphVertex currentVertex, Text otherValue);
}
interface IRelationshipComputation extends IComputation {
	
	public Text getRelationship();

	/**
	 * Does this vertex need an answer to the given question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation applies and should be run, false if it
	 * should be ignored
	 */
	public boolean isApplicable(KodeGraphVertex vertex);

	/**
	 * Does this vertex need more information populated before it can answer 
	 * the question?
	 * 
	 * @param vertex a reference to the Vertex
	 * @return true if computation should be postponed, false otherwise
	 */
	public boolean isWaitingOnInformation(KodeGraphVertex vertex);
	
	/**
	 * The value that will be sent to a candidate result 
	 * 
	 * @param vertex a reference to the source vertex
	 * @return the value
	 */
	public Text getComparableValue(KodeGraphVertex vertex);

	/**
	 * Find the candidate vertex which may be the answer 
	 * 
	 * @param vertexId The id of the current vertex
	 * @return The vertexId of the next candidate answer which a message
	 * will be sent to
	 */
	public Text getCandidate(Text vertexId);
	
	/**
	 * Given an incoming value, determine whether this vertex could be
	 * the next neighbour that the computation is looking for.
	 * 
	 * @param currentVertex the vertex which will be compared
	 * @param otherValue the value which the requesting vertex sent
	 * 
	 * @return true if the currentVertex can be considered a result
	 */
	public boolean amINextNeighbour(KodeGraphVertex currentVertex, Text otherValue);
}

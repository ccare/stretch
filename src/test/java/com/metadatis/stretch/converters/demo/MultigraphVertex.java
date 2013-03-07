package com.metadatis.stretch.converters.demo;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.giraph.graph.Edge;
import org.apache.giraph.graph.HashMapVertex;
import org.apache.giraph.graph.MutableVertex;
import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public abstract class MultigraphVertex<I extends WritableComparable, V extends Writable, E extends Writable, M extends Writable>
		extends MutableVertex<I, V, E, M> {
	
	private HashMapVertex<I, V, ArrayListWritable<E>, M> vertex = new HashMapVertex<I, V, ArrayListWritable<E>, M>() {

		@Override
		public void compute(Iterable<M> messages) throws IOException {
			MultigraphVertex.this.compute(messages);
		}
	};
	
	@Override
	public boolean addEdge(final I targetVertexId, final E value) {
		final ArrayListWritable<E> edgeValue;
		if (vertex.hasEdge(targetVertexId)) {
			edgeValue = vertex.getEdgeValue(targetVertexId);
			if (!edgeValue.contains(value)) {
				edgeValue.add(value);
			}
		} else {
			edgeValue = new ArrayListWritable<E>() {

				@Override
				public void setClass() {
					setClass((Class<E>) value.getClass());
				}
				
			};
			edgeValue.add(value);
		}
		return vertex.addEdge(targetVertexId, edgeValue);
	}

	@Override
	public E removeEdge(I targetVertexId) {
		ArrayListWritable<E> removedEdge = vertex.removeEdge(targetVertexId);
		return removedEdge.get(0);
	}

	@Override
	public void setEdges(Map<I, E> edges) {
		Map<I, ArrayListWritable<E>> myEdges = new HashMap<I, ArrayListWritable<E>>(edges.size());
		for (Entry<I, E> edge : edges.entrySet()) {
			I key = edge.getKey();
			E value = edge.getValue();
			if (myEdges.containsKey(key)) {
				ArrayListWritable<E> all = myEdges.get(key);
				all.add(value);
			}
		}		
	}

	@Override
	public abstract  void compute(Iterable<M> messages) throws IOException;
	
	@Override
	public Iterable<Edge<I, E>> getEdges() {
		Iterable<Edge<I, ArrayListWritable<E>>> edges = vertex.getEdges();
		final Iterator<Edge<I, ArrayListWritable<E>>> outer = edges.iterator();
		return new Iterable<Edge<I,E>>() {

			@Override
			public Iterator<Edge<I, E>> iterator() {
				// TODO Auto-generated method stub
				return new Iterator<Edge<I,E>>() {
					
					I currentTarget;
					Iterator<E> currentValues;

					@Override
					public boolean hasNext() {
						if (currentValues == null) {
							return outer.hasNext();
						} else {
							return currentValues.hasNext() || outer.hasNext();
						}
					}

					@Override
					public Edge<I, E> next() {
						if (currentValues == null || ! currentValues.hasNext()) {
							Edge<I, ArrayListWritable<E>> next = outer.next();
							currentTarget = next.getTargetVertexId();
							currentValues = next.getValue().iterator();
						}
						return new Edge<I, E>(currentTarget, currentValues.next());
					}

					@Override
					public void remove() {
						// TODO Auto-generated method stub
						
					}
				};
			}
		};
	}
	
}

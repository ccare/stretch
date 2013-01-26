package com.metadatis.stretch.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class GraphMessage implements Writable {

	private IntWritable type;
	private Text relationshipType;
	private Text vertex;
	private Text value;
			
	public Type getType() {
		char c = (char) type.get();
		return Type.fromCode(c);
	}
	
	public void setType(Type type) {
		this.type.set(type.code);
	}

	public Text getRelationshipType() {
		return relationshipType;
	}

	public void setRelationshipType(Text relationshipType) {
		this.relationshipType = relationshipType;
	}

	public Text getVertex() {
		return vertex;
	}

	public void setVertex(Text vertex) {
		this.vertex = vertex;
	}

	public Text getValue() {
		return value;
	}

	public void setValue(Text value) {
		this.value = value;
	}

	public static enum Type {
		
		ASK('Q'),
		ANSWER('A'),
		FIND('F'),
		FOUND('D'),
		ERROR('E');
		
		public final char code;
		
		Type(char code) {
			this.code = code;
		}
		
		public static final Type fromCode(char c) {
			for (Type t : values()) {
				if (t.code == c) {
					return t;
				}
			}
			return null;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		type.write(out);
		relationshipType.write(out);
		vertex.write(out);
		value.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type.readFields(in);
		relationshipType.readFields(in);
		vertex.readFields(in);
		value.readFields(in);
	}
}


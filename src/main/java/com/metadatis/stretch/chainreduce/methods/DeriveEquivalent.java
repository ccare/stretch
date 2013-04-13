package com.metadatis.stretch.chainreduce.methods;

import org.apache.hadoop.io.Text;

import com.metadatis.stretch.chainreduce.ChainReduceVertex;

public class DeriveEquivalent implements VertexMethod<Text> {
	
	@Override
	public Text calculate(
			ChainReduceVertex vertex, Object... args) {
		Text srcVertexId = (Text) args[0];
		Text result = (Text) args[1];
		return deriveEquivalentNode(srcVertexId, result.toString());
	}


	public Text deriveEquivalentNode(Text vertexId, String result) {
		String[] idSplit = vertexId.toString().split("/");
		String identifierFragment = idSplit[1];
		String candidate = String.format("%s/%s", result,
				identifierFragment);
		return new Text(candidate);
	}
}
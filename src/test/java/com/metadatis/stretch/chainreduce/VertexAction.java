package com.metadatis.stretch.chainreduce;

import java.io.IOException;

interface VertexAction {

	public boolean triggerable();

	public boolean finished();

	public void trigger() throws IOException;

	public boolean applicable();

}
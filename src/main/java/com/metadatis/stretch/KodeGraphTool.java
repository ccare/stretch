package com.metadatis.stretch;

import java.util.UUID;

import org.apache.giraph.GiraphConfiguration;
import org.apache.giraph.graph.GiraphJob;
import org.apache.giraph.io.GiraphFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KodeGraphTool implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setVertexClass(KodeGraphVertex.class);
		conf.setVertexInputFormatClass(KodeGraphVertexInputFormat.class);
		conf.setVertexOutputFormatClass(KodeGraphVertexOuputFormat.class);
		conf.setWorkerConfiguration(1, 1, 1.00F);
		conf.setBoolean(GiraphConfiguration.SPLIT_MASTER_WORKER, false);
		conf.set(FileOutputFormat.OUTDIR, "/tmp/ccare-out-" + UUID.randomUUID().toString());
//		conf.set(GiraphFileInputFormat.VERTEX_INPUT_DIR, "/tmp/ccare-in/vert");
//		conf.set(GiraphFileInputFormat.EDGE_INPUT_DIR, "/tmp/ccare-in/edge");
//		conf.set(GiraphFileInputFormat.INPUT_DIR, "/tmp/ccare-in/in");
	    conf.set(GiraphConfiguration.ZOOKEEPER_LIST, "localhost:22181"); 
	    conf.set(GiraphConfiguration.ZOOKEEPER_DIR, "/tmp/zkd");
	    conf.set(GiraphConfiguration.ZOOKEEPER_MANAGER_DIRECTORY, "/tmp/zkmanager");
	    conf.setInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
		
		GiraphJob job = new GiraphJob(conf, getClass().getName());	

        GiraphFileInputFormat.addVertexInputPath(job.getInternalJob(),
            new Path("/tmp/ccare-in/h1.adj"));
 
		    if (job.run(true) == true) {
		        return 0;
		    } else {
		        return -1;
		    }
	}

	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setConf(Configuration conf) {
		// TODO Auto-generated method stub
		
	}
	
	public static void main(String[] args) throws Exception {
		long start = System.currentTimeMillis();
		System.out.println("start " + start);
	    int i = ToolRunner.run(new KodeGraphTool(), args);
	    System.out.println("exit " + i);
	    System.out.println("Took " + (System.currentTimeMillis() - start));
	}

}

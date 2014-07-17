package com.github.projectflink.testPlan;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.hadoopcompatibility.mapred.record.HadoopDataSink;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;


public class DeflateWriterJob implements Program {

	public Plan getPlan(String... args) {
		String inputPath = args[0];
		String deflateOut = args[1];
		int dop = Integer.parseInt(args[2]);
		
		JobConf jobConf = new JobConf();

		FileDataSource src = new FileDataSource(TextInputFormat.class, inputPath);
		
		HadoopDataSink<Text, IntWritable> out = new HadoopDataSink<Text, IntWritable>(new org.apache.hadoop.mapred.TextOutputFormat<Text, IntWritable>(), jobConf, 
				"Hadoop TextOutputFormat (w/ deflate)", src, Text.class,IntWritable.class);
		
		org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(out.getJobConf(), new Path(deflateOut));
		
		jobConf.setBoolean("mapred.output.compress", true);
		
		Plan p = new Plan(out, "Deflate creat0r");
		p.setDefaultParallelism(dop);
		return p;
	}

	public static void main(String[] args) throws Exception {
		DeflateWriterJob sqT = new DeflateWriterJob();
		JobExecutionResult res = LocalExecutor.execute(sqT.getPlan(args));
		System.err.println("Result:\n"+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}

}

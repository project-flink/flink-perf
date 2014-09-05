package com.github.projectflink.testPlan;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem.WriteMode;


public class Readonly {
	public static void main(String[] args) throws Exception {
		
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		System.err.println("Using input="+args[0]);
		// get input data
		DataSet<String> text = env.readTextFile(args[0]);
		DataSet<String> res = text.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				return false;
			}
		});
		res.writeAsText("file:///tmp/out", WriteMode.OVERWRITE);
		
		// execute program
		env.execute("Read only job");
	}
}

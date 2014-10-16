package com.github.projectflink.grep;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrepJob {
	public static void main(String[] args) throws Exception {

		Pattern p = Pattern.compile("formulating");
		Matcher m = p.matcher("formulating");
		if(m.matches()) {
			System.out.println("match");
		}

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		System.err.println("Using input="+args[0]);
		System.err.println("Using output="+args[1]);
		// get input data
		DataSet<String> text = env.readTextFile(args[0]);
		DataSet<String> res = text.filter(new FilterFunction<String>() {
			@Override
			public boolean filter(String value) throws Exception {
				if(value == null || value.length() == 0) {
					return false;
				}
				Pattern p = Pattern.compile("formulating");
				Matcher m = p.matcher(value);
				return m.matches();
			}
		});
		res.writeAsText(args[1], FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("Read only job");
	}
}

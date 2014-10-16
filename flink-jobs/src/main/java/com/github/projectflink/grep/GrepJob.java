package com.github.projectflink.grep;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GrepJob {

	public static void main(String[] args) throws Exception {
		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		System.err.println("Using input="+args[0]);
		System.err.println("Using output="+args[1]);
		// get input data
		DataSet<String> text = env.readTextFile(args[0]);
		DataSet<String> res = text.filter(new RichFilterFunction<String>() {
			Pattern p = Pattern.compile("formulating");
			LongCounter filterMatches = new LongCounter();
			LongCounter filterRecords = new LongCounter();
			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				getRuntimeContext().addAccumulator("filterMatchCount", filterMatches);
				getRuntimeContext().addAccumulator("filterRecordCount", filterRecords);
			}

			@Override
			public boolean filter(String value) throws Exception {
				filterRecords.add(1L);
				if(value == null || value.length() == 0) {
					return false;
				}
				final Matcher m = p.matcher(value);
				if(m.find() ) {
					filterMatches.add(1L);
					return true;
				}
				return false;
			}
		});
		res.writeAsText(args[1], FileSystem.WriteMode.OVERWRITE);

		// execute program
		env.execute("Flink Filter benchmark");
	}
}

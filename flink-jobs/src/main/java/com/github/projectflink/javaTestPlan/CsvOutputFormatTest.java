package com.github.projectflink.javaTestPlan;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;



/**
 * Test to fix this issue
 * https://groups.google.com/forum/#!topic/stratosphere-dev/kZ3Thovg9HE
 *
 */
public class CsvOutputFormatTest{

	public static void main(String[] args) throws Exception {
		String input = "file:///tmp/word";
		String output = "file:///tmp/out";
		int dop = 15;

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple1<String>> src = env.readCsvFile(input).fieldDelimiter(';').types(String.class);
		src.writeAsCsv(output, "\n", ";");

		env.setParallelism(dop);
		env.execute();
	}

}

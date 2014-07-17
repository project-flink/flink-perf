package com.github.projectflink.testPlan;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.java.record.io.CsvInputFormat;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.types.StringValue;



/**
 * Test to fix this issue
 * https://groups.google.com/forum/#!topic/stratosphere-dev/kZ3Thovg9HE
 *
 */
public class CsvOutputFormatTest implements Program {
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	public Plan getPlan(String... args) {
		String input = args[0];
		String output = args[1];
		int dop = Integer.parseInt(args[2]);

		FileDataSource src = new FileDataSource(new CsvInputFormat(';', StringValue.class), input);
		FileDataSink sink = new FileDataSink(new CsvOutputFormat("\n", ";", StringValue.class), output, "output");
		sink.addInput(src);
		
		Plan p = new Plan(sink, "CsvOutputFormat Test");
		p.setDefaultParallelism(dop);
		return p;
	}

	public static void main(String[] args) throws Exception {
		CsvOutputFormatTest sqT = new CsvOutputFormatTest();
		JobExecutionResult res = LocalExecutor.execute(sqT.getPlan("file:///tmp/word", "file:///tmp/out","15"));
		System.err.println("Result:\n"+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}

}

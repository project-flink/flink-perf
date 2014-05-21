package eu.stratosphere.test.testPlan;


import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.StringValue;

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

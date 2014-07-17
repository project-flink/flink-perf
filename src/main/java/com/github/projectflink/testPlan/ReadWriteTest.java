package com.github.projectflink.testPlan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;

/**
 * Reads data from a directory and writes it again (to test reading deflate files)
 *
 */
public class ReadWriteTest implements Program {

	public static class SameIn extends MapFunction {
		
		private static int i = 0;
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
		//	System.err.println("Record "+ (i++) +" : "+record.getField(0, StringValue.class));
			
		}
		
	}
	public static class Counting extends MapFunction {

		LongCounter tup;
		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			tup = getRuntimeContext().getLongCounter("tuples");
		}
		@Override
		public void map(Record record, Collector<Record> out) throws Exception {
			// System.err.println("Line cont: "+record.getField(0, StringValue.class).getValue());
			tup.add(1L);
			out.collect(record);
		}
		
	}
	public Plan getPlan(String... args) {
		String in = args[0];
		String correct = args[1];
		String out = args[2];
		int dop = Integer.parseInt(args[3]);
		
		FileDataSource src = new FileDataSource(new TextInputFormat(), in);
		//FileDataSource correctSrc = new FileDataSource(new TextInputFormat(), correct);
		List<Operator> inL = new ArrayList(2);
		inL.add(src); //inL.add(correctSrc);
	//	MapOperator sameIn = MapOperator.builder(new SameIn() ).inputs( inL ).build();
		MapOperator cnt = MapOperator.builder(new Counting()).input(src).build();
		CsvOutputFormat cof = new CsvOutputFormat(StringValue.class);
		cof.setWriteMode(WriteMode.OVERWRITE);
		FileDataSink sink = new FileDataSink(cof, out);
		sink.setInput(cnt);
		
	//	FileDataSink fakeSink = new FileDataSink(cof, "file:///dev/null");
	//	fakeSink.setInput(sameIn);
		Plan p = new Plan((Collection)ImmutableSet.of(sink /*, fakeSink*/ ), "ReadWrite Test");
		p.setDefaultParallelism(dop);
		return p;
	}

	public static void main(String[] args) throws Exception {
		ReadWriteTest sqT = new ReadWriteTest();
		JobExecutionResult res = LocalExecutor.execute(sqT.getPlan(args));
		System.err.println("Result:\n"+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}

}

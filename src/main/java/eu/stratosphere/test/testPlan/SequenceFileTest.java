package eu.stratosphere.test.testPlan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.hadoopcompatibility.HadoopDataSink;
import eu.stratosphere.hadoopcompatibility.HadoopDataSource;
import eu.stratosphere.hadoopcompatibility.datatypes.WritableWrapperConverter;
import eu.stratosphere.test.testPlan.LargeTestPlan.CheckHadoop;
import eu.stratosphere.test.testPlan.LargeTestPlan.CheckHadoopWrapper;

public class SequenceFileTest implements Program {

	public Plan getPlan(String... args) {
		String sequenceFileInputPath = args[0];
		String seqFileOutPath = args[1];
		int dop = Integer.parseInt(args[2]);
		
		JobConf jobConf = new JobConf();
		FileInputFormat.addInputPath(jobConf, new Path(sequenceFileInputPath));
		//  with Stratosphere type converter
		HadoopDataSource hdsrc = new HadoopDataSource(new SequenceFileInputFormat<LongWritable, Text>(), jobConf, "Sequencefile");
		MapOperator checkHDsrc = MapOperator.builder(CheckHadoop.class).input(hdsrc).name("Check HDSrc output").build();
		
		HadoopDataSource hdsrcWrapperConverter = new HadoopDataSource(new SequenceFileInputFormat<LongWritable, Text>(), jobConf, "Sequencefile (WritableWrapper)", new WritableWrapperConverter());
		MapOperator checkHDsrcWrapperConverter = MapOperator.builder(new CheckHadoopWrapper(true)).input(hdsrcWrapperConverter).name("Check HDSrc output").build();
		// END: TEST 8

//		// don't use this for serious output. 
//		FileDataSink fakeSink = new FileDataSink(FailOutOutputFormat.class, "file:///tmp/fakeOut", "fake out");
//		fakeSink.addInput(checkHDsrc);
//		fakeSink.addInput(checkHDsrcWrapperConverter);
//		Plan p = new Plan(fakeSink, "Sequencefile Test");
		
		HadoopDataSink<Text, LongWritable> sink = new HadoopDataSink<Text, LongWritable>(
				new SequenceFileOutputFormat<Text, LongWritable>(), 
				checkHDsrcWrapperConverter, Text.class, LongWritable.class);
		TextOutputFormat.setOutputPath(sink.getJobConf(), new Path(seqFileOutPath));
		
		Plan p = new Plan(sink, "Sequencefile Test");
		p.setDefaultParallelism(dop);
		return p;
	}

	public static void main(String[] args) throws Exception {
		SequenceFileTest sqT = new SequenceFileTest();
		JobExecutionResult res = LocalExecutor.execute(sqT.getPlan(args));
		System.err.println("Result:\n"+AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
	}

}
